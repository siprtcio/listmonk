package manager

import (
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/knadh/listmonk/internal/i18n"
	"github.com/knadh/listmonk/logger"
	"github.com/knadh/listmonk/models"
)

const (
	// BaseTPL is the name of the base template.
	BaseTPL = "base"

	// ContentTpl is the name of the compiled message.
	ContentTpl = "content"

	dummyUUID = "00000000-0000-0000-0000-000000000000"
)

// Store represents a data backend, such as a database,
// that provides subscriber and campaign records.
type Store interface {
	NextCampaigns(currentIDs []int64, sentCounts []int64) ([]*models.Campaign, error)
	NextSubscribers(campID, limit int) ([]models.Subscriber, error)
	GetCampaign(campID int, authID string) (*models.Campaign, error)
	GetCampaignByAuthId(AuthID string) (*models.Campaign, error)
	GetAttachment(mediaID int, authID string) (models.Attachment, error)
	UpdateCampaignStatus(campID int, status string, authID string) error
	UpdateCampaignCounts(campID int, toSend int, sent int, lastSubID int) error
	CreateLink(url string, authID string) (string, error)
	BlocklistSubscriber(id int64) error
	DeleteSubscriber(id int64) error
	GetMessengerByAuthId(AuthID string, Messenger string) (string, error)
}

// Messenger is an interface for a generic messaging backend,
// for instance, e-mail, SMS etc.
type Messenger interface {
	Name() string
	Push(models.Message, string) error
	Flush() error
	Close() error
}

// CampStats contains campaign stats like per minute send rate.
type CampStats struct {
	SendRate int
}

// Manager handles the scheduling, processing, and queuing of campaigns
// and message pushes.
type Manager struct {
	cfg        Config
	store      Store
	i18n       *i18n.I18n
	messengers map[string]Messenger
	notifCB    models.AdminNotifCallback
	log        *log.Logger

	// Campaigns that are currently running.
	pipes    map[int]*pipe
	pipesMut sync.RWMutex

	tpls    map[int]*models.Template
	tplsMut sync.RWMutex

	// Links generated using Track() are cached here so as to not query
	// the database for the link UUID for every message sent. This has to
	// be locked as it may be used externally when previewing campaigns.
	links    map[string]string
	linksMut sync.RWMutex

	nextPipes chan *pipe
	campMsgQ  chan CampaignMessage
	msgQ      chan models.Message

	// Sliding window keeps track of the total number of messages sent in a period
	// and on reaching the specified limit, waits until the window is over before
	// sending further messages.
	slidingCount int
	slidingStart time.Time

	tplFuncs template.FuncMap

	activePipes sync.Map // Tracks campaigns currently being processed

}

// CampaignMessage represents an instance of campaign message to be pushed out,
// specific to a subscriber, via the campaign's messenger.
type CampaignMessage struct {
	Campaign   *models.Campaign
	Subscriber models.Subscriber

	from     string
	to       string
	subject  string
	body     []byte
	altBody  []byte
	unsubURL string

	pipe *pipe
}

// Config has parameters for configuring the manager.
type Config struct {
	// Number of subscribers to pull from the DB in a single iteration.
	BatchSize             int
	Concurrency           int
	MessageRate           int
	MaxSendErrors         int
	SlidingWindow         bool
	SlidingWindowDuration time.Duration
	SlidingWindowRate     int
	RequeueOnError        bool
	FromEmail             string
	IndividualTracking    bool
	LinkTrackURL          string
	UnsubURL              string
	OptinURL              string
	MessageURL            string
	ViewTrackURL          string
	ArchiveURL            string
	RootURL               string
	UnsubHeader           bool

	// Interval to scan the DB for active campaign checkpoints.
	ScanInterval time.Duration

	// ScanCampaigns indicates whether this instance of manager will scan the DB
	// for active campaigns and process them.
	// This can be used to run multiple instances of listmonk
	// (exposed to the internet, private etc.) where only one does campaign
	// processing while the others handle other kinds of traffic.
	ScanCampaigns bool
}

type msgError struct {
	st  *pipe
	err error
}

type campaignState struct {
	inProgress bool
	mu         sync.Mutex
}

var pushTimeout = time.Second * 3

// New returns a new instance of Mailer.
func New(cfg Config, store Store, notifCB models.AdminNotifCallback, i *i18n.I18n, l *log.Logger) *Manager {
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 1000
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.MessageRate < 1 {
		cfg.MessageRate = 1
	}

	m := &Manager{
		cfg:          cfg,
		store:        store,
		i18n:         i,
		notifCB:      notifCB,
		log:          l,
		messengers:   make(map[string]Messenger),
		pipes:        make(map[int]*pipe),
		tpls:         make(map[int]*models.Template),
		links:        make(map[string]string),
		nextPipes:    make(chan *pipe, 1000),
		campMsgQ:     make(chan CampaignMessage, cfg.Concurrency*cfg.MessageRate*2),
		msgQ:         make(chan models.Message, cfg.Concurrency*cfg.MessageRate*2),
		slidingStart: time.Now(),
	}
	m.tplFuncs = m.makeGnericFuncMap()

	return m
}

// AddMessenger adds a Messenger messaging backend to the manager.
func (m *Manager) AddMessenger(msg Messenger) error {
	id := msg.Name()
	if _, ok := m.messengers[id]; ok {
		return fmt.Errorf("messenger '%s' is already loaded", id)
	}
	m.messengers[id] = msg
	return nil
}

// PushMessage pushes an arbitrary non-campaign Message to be sent out by the workers.
// It times out if the queue is busy.
func (m *Manager) PushMessage(msg models.Message) error {
	t := time.NewTicker(pushTimeout)
	defer t.Stop()

	select {
	case m.msgQ <- msg:
	case <-t.C:
		m.log.Printf("message push timed out: '%s'", msg.Subject)
		return errors.New("message push timed out")
	}
	return nil
}

// PushCampaignMessage pushes a campaign messages into a queue to be sent out by the workers.
// It times out if the queue is busy.
func (m *Manager) PushCampaignMessage(msg CampaignMessage) error {
	t := time.NewTicker(pushTimeout)
	defer t.Stop()

	// Load any media/attachments.
	if err := m.attachMedia(msg.Campaign); err != nil {
		return err
	}

	select {
	case m.campMsgQ <- msg:
	case <-t.C:
		m.log.Printf("message push timed out: '%s'", msg.Subject())
		return errors.New("message push timed out")
	}
	return nil
}

// HasMessenger checks if a given messenger is registered.
func (m *Manager) HasMessenger(id string) bool {
	_, ok := m.messengers[id]
	return ok
}

// HasRunningCampaigns checks if there are any active campaigns.
func (m *Manager) HasRunningCampaigns() bool {
	m.pipesMut.Lock()
	defer m.pipesMut.Unlock()
	return len(m.pipes) > 0
}

// GetCampaignStats returns campaign statistics.
func (m *Manager) GetCampaignStats(id int) CampStats {
	n := 0

	m.pipesMut.Lock()
	if c, ok := m.pipes[id]; ok {
		n = int(c.rate.Rate())
	}
	m.pipesMut.Unlock()

	return CampStats{SendRate: n}
}

// Run is a blocking function (that should be invoked as a goroutine)
// that scans the data source at regular intervals for pending campaigns,
// and queues them for processing. The process queue fetches batches of
// subscribers and pushes messages to them for each queued campaign
// until all subscribers are exhausted, at which point, a campaign is marked
// as "finished".
func (m *Manager) Run() {
	if m.cfg.ScanCampaigns {
		// Periodically scan campaigns and push running campaigns to nextPipes
		// to fetch subscribers from the campaign.
		go m.scanCampaigns(m.cfg.ScanInterval)
	}

	// Spawn N message workers.
	for i := 0; i < m.cfg.Concurrency; i++ {
		go m.worker()
	}

	// Indefinitely wait on the pipe queue to fetch the next set of subscribers
	// for any active campaigns.
	for p := range m.nextPipes {
		has, err := p.NextSubscribers()
		if err != nil {
			m.log.Printf("error processing campaign batch (%s): %v", p.camp.Name, err)
			continue
		}

		if has {
			// There are more subscribers to fetch. Queue again.
			select {
			case m.nextPipes <- p:
			default:
			}
		} else {
			// Mark the pseudo counter that's added in makePipe() that is used
			// to force a wait on a pipe.
			p.wg.Done()
		}
	}
}

// CacheTpl caches a template for ad-hoc use. This is currently only used by tx templates.
func (m *Manager) CacheTpl(id int, tpl *models.Template) {
	m.tplsMut.Lock()
	m.tpls[id] = tpl
	m.tplsMut.Unlock()
}

// DeleteTpl deletes a cached template.
func (m *Manager) DeleteTpl(id int) {
	m.tplsMut.Lock()
	delete(m.tpls, id)
	m.tplsMut.Unlock()
}

// GetTpl returns a cached template.
func (m *Manager) GetTpl(id int) (*models.Template, error) {
	m.tplsMut.RLock()
	tpl, ok := m.tpls[id]
	m.tplsMut.RUnlock()

	if !ok {
		return nil, fmt.Errorf("template %d not found", id)
	}

	return tpl, nil
}

// TemplateFuncs returns the template functions to be applied into
// compiled campaign templates.
func (m *Manager) TemplateFuncs(c *models.Campaign) template.FuncMap {
	f := template.FuncMap{
		"TrackLink": func(url string, msg *CampaignMessage) string {
			subUUID := msg.Subscriber.UUID
			if !m.cfg.IndividualTracking {
				subUUID = dummyUUID
			}

			return m.trackLink(url, msg.Campaign.UUID, subUUID, c.AuthID)
		},
		"TrackView": func(msg *CampaignMessage) template.HTML {
			subUUID := msg.Subscriber.UUID
			if !m.cfg.IndividualTracking {
				subUUID = dummyUUID
			}

			return template.HTML(fmt.Sprintf(`<img src="%s" alt="" />`,
				fmt.Sprintf(m.cfg.ViewTrackURL, msg.Campaign.UUID, subUUID)))
		},
		"UnsubscribeURL": func(msg *CampaignMessage) string {
			return msg.unsubURL
		},
		"ManageURL": func(msg *CampaignMessage) string {
			return msg.unsubURL + "?manage=true"
		},
		"OptinURL": func(msg *CampaignMessage) string {
			// Add list IDs.
			// TODO: Show private lists list on optin e-mail
			return fmt.Sprintf(m.cfg.OptinURL, msg.Subscriber.UUID, "")
		},
		"MessageURL": func(msg *CampaignMessage) string {
			return fmt.Sprintf(m.cfg.MessageURL, c.UUID, msg.Subscriber.UUID)
		},
		"ArchiveURL": func() string {
			return m.cfg.ArchiveURL
		},
		"RootURL": func() string {
			return m.cfg.RootURL
		},
	}

	for k, v := range m.tplFuncs {
		f[k] = v
	}

	return f
}

func (m *Manager) GenericTemplateFuncs() template.FuncMap {
	return m.tplFuncs
}

// StopCampaign marks a running campaign as stopped so that all its queued messages are ignored.
func (m *Manager) StopCampaign(id int) {
	m.pipesMut.RLock()
	if p, ok := m.pipes[id]; ok {
		p.Stop(false)
	}
	m.pipesMut.RUnlock()
}

// Close closes and exits the campaign manager.
func (m *Manager) Close() {
	close(m.nextPipes)
	close(m.msgQ)
}

// scanCampaigns is a blocking function that periodically scans the data source
// for campaigns to process and dispatches them to the manager. It feeds campaigns
// into nextPipes.
func (m *Manager) scanCampaigns(tick time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()

	for {
		select {
		// Periodically scan the data source for campaigns to process.
		case <-t.C:
			ids, counts := m.getCurrentCampaigns()
			campaigns, err := m.store.NextCampaigns(ids, counts)
			if err != nil {
				m.log.Printf("error fetching campaigns: %v", err)
				continue
			}

			for _, c := range campaigns {
				val, exists := m.activePipes.Load(c.ID)
				var state *campaignState
				if exists {
					state = val.(*campaignState)
				} else {
					state = &campaignState{}
					m.activePipes.Store(c.ID, state)
				}

				// Lock state and check if the campaign is already being processed
				state.mu.Lock()
				if state.inProgress {
					state.mu.Unlock()
					m.log.Printf("Campaign %s is already being processed, skipping.", c.Name)
					continue // Skip if already being processed
				}

				// Mark campaign as in progress
				state.inProgress = true
				state.mu.Unlock()

				// Start processing the campaign in a separate goroutine
				go func(c *models.Campaign) {
					defer func() {
						// Ensure cleanup after processing
						state.mu.Lock()
						state.inProgress = false
						state.mu.Unlock()
					}()

					// Log that the campaign is being processed
					logger.Info("Start processing campaign", logger.LogFields{
						"Campaign Name": c.Name,
					})
					p, err := m.newPipe(c)
					if err != nil {
						logger.Error("error processing campaign", logger.LogFields{
							"Campaign Name": c.Name,
							"Error":         err,
						})
						return
					}
					select {
					case m.nextPipes <- p:
					default:
					}
				}(c) // Process the campaign in a separate goroutine
			}
		}
	}
}

// worker is a blocking function that perpetually listents to events (message) on different
// queues and processes them.
func (m *Manager) worker() {
	// Counter to keep track of the message / sec rate limit.
	numMsg := 0
	for {
		select {
		// Campaign message.
		case msg, ok := <-m.campMsgQ:
			if !ok {
				return
			}

			// If the campaign has ended, ignore the message.
			if msg.pipe != nil && msg.pipe.stopped.Load() {
				msg.pipe.wg.Done()
				continue
			}

			// Pause on hitting the message rate.
			if numMsg >= m.cfg.MessageRate {
				time.Sleep(time.Second)
				numMsg = 0
			}
			numMsg++

			// Outgoing message.
			out := models.Message{
				From:        msg.from,
				To:          []string{msg.to},
				Subject:     msg.subject,
				ContentType: msg.Campaign.ContentType,
				Body:        msg.body,
				AltBody:     msg.altBody,
				Subscriber:  msg.Subscriber,
				Campaign:    msg.Campaign,
				Attachments: msg.Campaign.Attachments,
			}

			h := textproto.MIMEHeader{}
			h.Set(models.EmailHeaderCampaignUUID, msg.Campaign.UUID)
			h.Set(models.EmailHeaderSubscriberUUID, msg.Subscriber.UUID)

			// Attach List-Unsubscribe headers?
			if m.cfg.UnsubHeader {
				h.Set("List-Unsubscribe-Post", "List-Unsubscribe=One-Click")
				h.Set("List-Unsubscribe", `<`+msg.unsubURL+`>`)
			}

			// Attach any custom headers.
			if len(msg.Campaign.Headers) > 0 {
				for _, set := range msg.Campaign.Headers {
					for hdr, val := range set {
						h.Add(hdr, val)
					}
				}
			}

			out.Headers = h

			var rootUrl string
			var err error
			if msg.Campaign.Messenger != "email" {
				rootUrl, err = m.store.GetMessengerByAuthId(msg.Campaign.AuthID, msg.Campaign.Messenger)
				if err != nil {
					logger.Error("Error fetching messenger in campaign", logger.LogFields{
						"Campaign Name": msg.Campaign.Name,
						"Error":         err,
					})
				}
				logger.Info("Sending message in campaign", logger.LogFields{
					"Campaign Name": msg.Campaign.Name,
					"Root URL":      rootUrl,
				})
			}
			err = m.messengers[msg.Campaign.Messenger].Push(out, rootUrl)
			if err != nil {
				logger.Error("Error sending message in campaign for subscriber", logger.LogFields{
					"Campaign Name": msg.Campaign.Name,
					"Subscriber":    msg.Subscriber.ID,
					"Error":         err,
				})
			}

			// Increment the send rate or the error counter if there was an error.
			if msg.pipe != nil {
				// Mark the message as done.
				msg.pipe.wg.Done()

				if err != nil {
					msg.pipe.OnError()
				} else {
					id := uint64(msg.Subscriber.ID)
					if id > msg.pipe.lastID.Load() {
						msg.pipe.lastID.Store(uint64(msg.Subscriber.ID))
					}
					msg.pipe.rate.Incr(1)
					msg.pipe.sent.Add(1)
				}
			}

		// Arbitrary message.
		case msg, ok := <-m.msgQ:
			if !ok {
				return
			}

			var rootUrl string
			var err error
			if msg.Campaign.Messenger != "email" {
				rootUrl, err = m.store.GetMessengerByAuthId(msg.Campaign.AuthID, msg.Campaign.Messenger)
				if err != nil {
					logger.Error("Error fetching messenger in campaign", logger.LogFields{
						"Campaign Name": msg.Campaign.Name,
						"Error":         err,
					})
				}
				logger.Info("Sending message in campaign", logger.LogFields{
					"Campaign Name": msg.Campaign.Name,
					"Root URL":      rootUrl,
				})
			}
			err = m.messengers[msg.Messenger].Push(msg, rootUrl)
			if err != nil {
				logger.Error("Error sending message in campaign for subscriber", logger.LogFields{
					"Campaign Name": msg.Campaign.Name,
					"Subscriber":    msg.Subscriber.ID,
					"Error":         err,
				})
			}
		}
	}
}

// getRunningCampaignIDs returns the IDs of campaigns currently being processed.
func (m *Manager) getRunningCampaignIDs() []int64 {
	// Needs to return an empty slice in case there are no campaigns.
	m.pipesMut.RLock()
	ids := make([]int64, 0, len(m.pipes))
	for _, p := range m.pipes {
		ids = append(ids, int64(p.camp.ID))
	}
	m.pipesMut.RUnlock()
	return ids
}

// getCurrentCampaigns returns the IDs of campaigns currently being processed
// and their sent counts.
func (m *Manager) getCurrentCampaigns() ([]int64, []int64) {
	// Needs to return an empty slice in case there are no campaigns.
	m.pipesMut.RLock()
	defer m.pipesMut.RUnlock()

	var (
		ids    = make([]int64, 0, len(m.pipes))
		counts = make([]int64, 0, len(m.pipes))
	)
	for _, p := range m.pipes {
		ids = append(ids, int64(p.camp.ID))

		// Get the sent counts for campaigns and reset them to 0
		// as in the database, they're stored cumulatively (sent += $newSent).
		counts = append(counts, p.sent.Load())
		p.sent.Store(0)
	}

	return ids, counts
}

// isCampaignProcessing checks if the campaign is being processed.
func (m *Manager) isCampaignProcessing(id int) bool {
	m.pipesMut.RLock()
	_, ok := m.pipes[id]
	m.pipesMut.RUnlock()
	return ok
}

// trackLink register a URL and return its UUID to be used in message templates
// for tracking links.
func (m *Manager) trackLink(url, campUUID, subUUID string, authID string) string {
	url = strings.ReplaceAll(url, "&amp;", "&")

	m.linksMut.RLock()
	if uu, ok := m.links[url]; ok {
		m.linksMut.RUnlock()
		return fmt.Sprintf(m.cfg.LinkTrackURL, uu, campUUID, subUUID)
	}
	m.linksMut.RUnlock()

	// Register link.
	uu, err := m.store.CreateLink(url, authID)
	if err != nil {
		m.log.Printf("error registering tracking for link '%s': %v", url, err)

		// If the registration fails, fail over to the original URL.
		return url
	}

	m.linksMut.Lock()
	m.links[url] = uu
	m.linksMut.Unlock()

	return fmt.Sprintf(m.cfg.LinkTrackURL, uu, campUUID, subUUID)
}

// sendNotif sends a notification to registered admin e-mails.
func (m *Manager) sendNotif(c *models.Campaign, status, reason string) error {
	var (
		subject = fmt.Sprintf("%s: %s", strings.Title(status), c.Name)
		data    = map[string]interface{}{
			"ID":     c.ID,
			"Name":   c.Name,
			"Status": status,
			"Sent":   c.Sent,
			"ToSend": c.ToSend,
			"Reason": reason,
		}
	)
	return m.notifCB(subject, data)
}

func (m *Manager) makeGnericFuncMap() template.FuncMap {
	f := template.FuncMap{
		"Date": func(layout string) string {
			if layout == "" {
				layout = time.ANSIC
			}
			return time.Now().Format(layout)
		},
		"L": func() *i18n.I18n {
			return m.i18n
		},
		"Safe": func(safeHTML string) template.HTML {
			return template.HTML(safeHTML)
		},
	}

	for k, v := range sprig.GenericFuncMap() {
		f[k] = v
	}

	return f
}

func (m *Manager) attachMedia(c *models.Campaign) error {
	// Load any media/attachments.
	for _, mid := range []int64(c.MediaIDs) {
		a, err := m.store.GetAttachment(int(mid), c.AuthID)
		if err != nil {
			return fmt.Errorf("error fetching attachment %d on campaign %s: %v", mid, c.Name, err)
		}

		c.Attachments = append(c.Attachments, a)
	}

	return nil
}

// MakeAttachmentHeader is a helper function that returns a
// textproto.MIMEHeader tailored for attachments, primarily
// email. If no encoding is given, base64 is assumed.
func MakeAttachmentHeader(filename, encoding, contentType string) textproto.MIMEHeader {
	if encoding == "" {
		encoding = "base64"
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", "attachment; filename="+filename)
	h.Set("Content-Type", fmt.Sprintf("%s; name=\""+filename+"\"", contentType))
	h.Set("Content-Transfer-Encoding", encoding)
	return h
}
