package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/knadh/listmonk/models"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
	"gopkg.in/volatiletech/null.v6"
)

// campaignReq is a wrapper over the Campaign model for receiving
// campaign creation and update data from APIs.
type campaignReq struct {
	models.Campaign

	// Indicates if the "send_at" date should be written or set to null.
	SendLater bool `json:"send_later"`

	// This overrides Campaign.Lists to receive and
	// write a list of int IDs during creation and updation.
	// Campaign.Lists is JSONText for sending lists children
	// to the outside world.
	ListIDs []int `json:"lists"`

	MediaIDs []int `json:"media"`

	// This is only relevant to campaign test requests.
	SubscriberEmails pq.StringArray `json:"subscribers"`
	VoiceOption      string         `json:"voice_option"`
}

// campaignContentReq wraps params coming from API requests for converting
// campaign content formats.
type campaignContentReq struct {
	models.Campaign
	From string `json:"from"`
	To   string `json:"to"`
}

var (
	regexFromAddress = regexp.MustCompile(`((.+?)\s)?<(.+?)@(.+?)>`)
	regexSlug        = regexp.MustCompile(`[^\p{L}\p{M}\p{N}]`)
)

// handleGetCampaigns handles retrieval of campaigns.
func handleGetCampaigns(c echo.Context) error {
	var (
		app = c.Get("app").(*App)
		pg  = app.paginator.NewFromURL(c.Request().URL.Query())

		status    = c.QueryParams()["status"]
		tags      = c.QueryParams()["tag"]
		query     = strings.TrimSpace(c.FormValue("query"))
		orderBy   = c.FormValue("order_by")
		order     = c.FormValue("order")
		noBody, _ = strconv.ParseBool(c.QueryParam("no_body"))
	)

	// Retrieve authid from headers (adjust header key if needed)
	authID := c.Request().Header.Get("X-Auth-ID") // Or any other header key where authid is stored
	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	res, total, err := app.core.QueryCampaigns(query, status, tags, orderBy, order, pg.Offset, pg.Limit, authID)
	if err != nil {
		return err
	}

	if noBody {
		for i := 0; i < len(res); i++ {
			res[i].Body = ""
		}
	}

	var out models.PageResults
	if len(res) == 0 {
		out.Results = []models.Campaign{}
		return c.JSON(http.StatusOK, okResp{out})
	}

	// Meta.
	out.Query = query
	out.Results = res
	out.Total = total
	out.Page = pg.Page
	out.PerPage = pg.PerPage

	return c.JSON(http.StatusOK, okResp{out})
}

// handleGetCampaign handles retrieval of campaigns.
func handleGetCampaign(c echo.Context) error {
	var (
		app       = c.Get("app").(*App)
		id, err   = strconv.Atoi(c.Param("id"))
		noBody, _ = strconv.ParseBool(c.QueryParam("no_body"))
	)
	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	out, err := app.core.GetCampaign(id, "", "", authID)
	if err != nil {
		return err
	}

	if noBody {
		out.Body = ""
	}

	return c.JSON(http.StatusOK, okResp{out})
}

func handleGetCampaignByAuthId(c echo.Context) error {
	var (
		app       = c.Get("app").(*App)
		noBody, _ = strconv.ParseBool(c.QueryParam("no_body"))
		authID    = c.Param("authid")
	)

	// Attempt to retrieve the campaigns by AuthID
	out, err := app.core.GetCampaignByAuthId(authID)

	if err != nil {
		// Log the error details
		app.log.Printf("Error fetching campaigns with AuthID: %s Error: %v", authID, err)

		// Return internal server error for other cases
		return echo.NewHTTPError(http.StatusInternalServerError, "Error retrieving campaigns")
	}

	// Log successful retrieval of the campaigns
	app.log.Printf("Successfully retrieved %d campaigns with AuthID: %s", len(out), authID)

	// If the "no_body" query parameter is set, clear the body content from each campaign
	if noBody {
		app.log.Printf("NoBody flag is true, clearing the campaign body content")
		for i := range out {
			out[i].Body = "" // Clear the body for each campaign
		}
	}

	// Return the campaigns data as JSON
	return c.JSON(http.StatusOK, okResp{out})
}

// handlePreviewCampaign renders the HTML preview of a campaign body.
func handlePreviewCampaign(c echo.Context) error {
	var (
		app      = c.Get("app").(*App)
		id, _    = strconv.Atoi(c.Param("id"))
		tplID, _ = strconv.Atoi(c.FormValue("template_id"))
	)
	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	if id < 1 {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("globals.messages.invalidID"))
	}
	initSettings("SELECT JSON_OBJECT_AGG(key, value) AS settings FROM settings WHERE authid = $1;", db, ko, authID)
	initMediaStore()
	app.manager = initCampaignManager(app.queries, app.constants, app)
	app.messengers[emailMsgr] = initSMTPMessenger(app.manager)
	for _, m := range initPostbackMessengers(app.manager) {
		app.messengers[m.Name()] = m
	}
	for _, m := range app.messengers {
		app.manager.AddMessenger(m)
	}

	camp, err := app.core.GetCampaignForPreview(id, tplID, authID)
	if err != nil {
		return err
	}

	// There's a body in the request to preview instead of the body in the DB.
	if c.Request().Method == http.MethodPost {
		camp.ContentType = c.FormValue("content_type")
		camp.Body = c.FormValue("body")
	}

	// Use a dummy campaign ID to prevent views and clicks from {{ TrackView }}
	// and {{ TrackLink }} being registered on preview.
	camp.UUID = dummySubscriber.UUID
	if err := camp.CompileTemplate(app.manager.TemplateFuncs(&camp)); err != nil {
		app.log.Printf("error compiling template: %v", err)
		return echo.NewHTTPError(http.StatusBadRequest,
			app.i18n.Ts("templates.errorCompiling", "error", err.Error()))
	}

	// Render the message body.
	msg, err := app.manager.NewCampaignMessage(&camp, dummySubscriber)
	if err != nil {
		app.log.Printf("error rendering message: %v", err)
		return echo.NewHTTPError(http.StatusBadRequest,
			app.i18n.Ts("templates.errorRendering", "error", err.Error()))
	}

	if camp.ContentType == models.CampaignContentTypePlain {
		return c.String(http.StatusOK, string(msg.Body()))
	}

	return c.HTML(http.StatusOK, string(msg.Body()))
}

// handleCampaignContent handles campaign content (body) format conversions.
func handleCampaignContent(c echo.Context) error {
	var (
		app   = c.Get("app").(*App)
		id, _ = strconv.Atoi(c.Param("id"))
	)
	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	if id < 1 {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("globals.messages.invalidID"))
	}

	var camp campaignContentReq
	if err := c.Bind(&camp); err != nil {
		return err
	}

	out, err := camp.ConvertContent(camp.From, camp.To, authID)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	return c.JSON(http.StatusOK, okResp{out})
}

// handleCreateCampaign handles campaign creation.
// Newly created campaigns are always drafts.
// handleCreateCampaign handles campaign creation.
// Newly created campaigns are always drafts.
func handleCreateCampaign(c echo.Context) error {
	var (
		app = c.Get("app").(*App)
		o   campaignReq
	)

	// Extract the authid from the URL
	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	initSettings("SELECT JSON_OBJECT_AGG(key, value) AS settings FROM settings WHERE authid = $1;", db, ko, authID)
	initMediaStore()
	app.manager = initCampaignManager(app.queries, app.constants, app)
	app.messengers[emailMsgr] = initSMTPMessenger(app.manager)
	for _, m := range initPostbackMessengers(app.manager) {
		app.messengers[m.Name()] = m
	}
	for _, m := range app.messengers {
		app.manager.AddMessenger(m)
	}

	if err := c.Bind(&o); err != nil {
		return err
	}

	// Prepare default values and validate fields
	if o.Type == models.CampaignTypeOptin {
		op, err := makeOptinCampaignMessage(o, app)
		if err != nil {
			return err
		}
		o = op
	} else if o.Type == "" {
		o.Type = models.CampaignTypeRegular
	}

	if o.ContentType == "" {
		o.ContentType = models.CampaignContentTypeRichtext
	}

	if o.Messenger == "" {
		o.Messenger = "email" // Default messenger
	}

	// Validate common campaign fields
	if c, err := validateCampaignFields(o, app); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	} else {
		o = c
	}

	// Initialize new fields for voice campaigns
	if o.Messenger == "voice" {
		if o.VoiceOption == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "Voice option is required for voice campaigns")
		}

		// Validate the voice option and set the appropriate fields
		switch o.VoiceOption {
		case "template":
			// Handle template option (Ensure template ID is provided)
			o.Body = "-" // Not required for template option
		case "music":
			// Handle music option
			if o.MusicID == "" {
				return echo.NewHTTPError(http.StatusBadRequest, "Music ID is required for music option.")
			}
			o.Body = "-" // Assuming Body is not required for music
		case "text-to-speech":
			// Handle text-to-speech option
			if o.Body == "" {
				return echo.NewHTTPError(http.StatusBadRequest, "Body text is required for text-to-speech option.")
			}
			// Set additional fields like Vendor and Language if needed
			if o.Vendor == "" {
				o.Vendor = "aws" // Default vendor
			}
			if o.Language == "" {
				o.Language = "en-US" // Default language
			}
		default:
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid voice option provided.")
		}
	} else {
		// If messenger is email or SMS, leave new fields empty
		o.MusicID = "-"
		o.Vendor = "-"
		o.Loop = 0
		o.Body = "-"
		o.Language = "-"
	}

	// Check and set ArchiveTemplateID if not provided
	if o.ArchiveTemplateID == 0 {
		o.ArchiveTemplateID = o.TemplateID
	}

	// Call CreateCampaign with the required parameters, including the voice option
	out, err := app.core.CreateCampaign(o.Campaign, o.ListIDs, o.MediaIDs, authID, o.VoiceOption)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, okResp{out})
}

// handleUpdateCampaign handles campaign modification.o.AuthID = c.Param("authid")
// Campaigns that are done cannot be modified.
func handleUpdateCampaign(c echo.Context) error {
	var (
		app   = c.Get("app").(*App)
		id, _ = strconv.Atoi(c.Param("id"))
	)
	authID := c.Request().Header.Get("X-Auth-ID") // Or any other header key where authid is stored
	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	if id < 1 {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("globals.messages.invalidID"))

	}

	cm, err := app.core.GetCampaign(id, "", "", authID)
	if err != nil {
		return err
	}

	if isCampaignalMutable(cm.Status) {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("campaigns.cantUpdate"))
	}

	initSettings("SELECT JSON_OBJECT_AGG(key, value) AS settings FROM settings WHERE authid = $1;", db, ko, authID)
	initMediaStore()
	app.manager = initCampaignManager(app.queries, app.constants, app)
	app.messengers[emailMsgr] = initSMTPMessenger(app.manager)
	for _, m := range initPostbackMessengers(app.manager) {
		app.messengers[m.Name()] = m
	}
	for _, m := range app.messengers {
		app.manager.AddMessenger(m)
	}

	// Read the incoming params into the existing campaign fields from the DB.
	// This allows updating of values that have been sent whereas fields
	// that are not in the request retain the old values.
	o := campaignReq{Campaign: cm}
	if err := c.Bind(&o); err != nil {
		return err
	}

	if c, err := validateCampaignFields(o, app); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	} else {
		o = c
	}

	out, err := app.core.UpdateCampaign(id, o.Campaign, o.ListIDs, o.MediaIDs, o.SendLater, authID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, okResp{out})
}

// handleUpdateCampaignStatus handles campaign status modification.
func handleUpdateCampaignStatus(c echo.Context) error {
	var (
		app   = c.Get("app").(*App)
		id, _ = strconv.Atoi(c.Param("id"))
	)
	authID := c.Request().Header.Get("X-Auth-ID") // Or any other header key where authid is stored
	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	if id < 1 {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("globals.messages.invalidID"))
	}

	var o struct {
		Status string `json:"status"`
	}

	if err := c.Bind(&o); err != nil {
		return err
	}

	out, err := app.core.UpdateCampaignStatus(id, o.Status, authID)
	if err != nil {
		return err
	}

	initSettings("SELECT JSON_OBJECT_AGG(key, value) AS settings FROM settings WHERE authid = $1;", db, ko, authID)
	initMediaStore()
	app.manager = initCampaignManager(app.queries, app.constants, app)
	app.messengers[emailMsgr] = initSMTPMessenger(app.manager)
	for _, m := range initPostbackMessengers(app.manager) {
		app.messengers[m.Name()] = m
	}
	for _, m := range app.messengers {
		app.manager.AddMessenger(m)
	}

	go app.manager.Run()

	if o.Status == models.CampaignStatusPaused || o.Status == models.CampaignStatusCancelled {
		app.manager.StopCampaign(id)
	}

	return c.JSON(http.StatusOK, okResp{out})
}

// handleUpdateCampaignArchive handles campaign status modification.
func handleUpdateCampaignArchive(c echo.Context) error {
	var (
		app   = c.Get("app").(*App)
		id, _ = strconv.Atoi(c.Param("id"))
	)
	authID := c.Request().Header.Get("X-Auth-ID") // Or any other header key where authid is stored
	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	req := struct {
		Archive     bool        `json:"archive"`
		TemplateID  int         `json:"archive_template_id"`
		Meta        models.JSON `json:"archive_meta"`
		ArchiveSlug string      `json:"archive_slug"`
	}{}

	// Get and validate fields.
	if err := c.Bind(&req); err != nil {
		return err
	}

	if req.ArchiveSlug != "" {
		// Format the slug to be alpha-numeric-dash.
		s := strings.ToLower(req.ArchiveSlug)
		s = strings.TrimSpace(regexSlug.ReplaceAllString(s, " "))
		s = regexpSpaces.ReplaceAllString(s, "-")
		req.ArchiveSlug = s
	}

	if err := app.core.UpdateCampaignArchive(id, req.Archive, req.TemplateID, req.Meta, req.ArchiveSlug, authID); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, okResp{req})
}

// handleDeleteCampaign handles campaign deletion.
// Only scheduled campaigns that have not started yet can be deleted.
func handleDeleteCampaign(c echo.Context) error {
	var (
		app   = c.Get("app").(*App)
		id, _ = strconv.Atoi(c.Param("id"))
	)
	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	if id < 1 {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("globals.messages.invalidID"))
	}

	if err := app.core.DeleteCampaign(id, authID); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, okResp{true})
}

// handleGetRunningCampaignStats returns stats of a given set of campaign IDs.
func handleGetRunningCampaignStats(c echo.Context) error {
	var (
		app = c.Get("app").(*App)
	)
	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	out, err := app.core.GetRunningCampaignStats(authID)
	if err != nil {
		return err
	}

	if len(out) == 0 {
		return c.JSON(http.StatusOK, okResp{[]struct{}{}})
	}

	// Compute rate.
	for i, c := range out {
		if c.Started.Valid && c.UpdatedAt.Valid {
			diff := int(c.UpdatedAt.Time.Sub(c.Started.Time).Minutes())
			if diff < 1 {
				diff = 1
			}

			rate := c.Sent / diff
			if rate > c.Sent || rate > c.ToSend {
				rate = c.Sent
			}

			// Rate since the starting of the campaign.
			out[i].NetRate = rate

			// Realtime running rate over the last minute.
			out[i].Rate = app.manager.GetCampaignStats(c.ID).SendRate
		}
	}

	return c.JSON(http.StatusOK, okResp{out})
}

// handleTestCampaign handles the sending of a campaign message to
// arbitrary subscribers for testing.
func handleTestCampaign(c echo.Context) error {
	var (
		app       = c.Get("app").(*App)
		campID, _ = strconv.Atoi(c.Param("id"))
		tplID, _  = strconv.Atoi(c.FormValue("template_id"))
		req       campaignReq
	)
	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	if campID < 1 {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("globals.messages.errorID"))
	}

	initSettings("SELECT JSON_OBJECT_AGG(key, value) AS settings FROM settings WHERE authid = $1;", db, ko, authID)
	initMediaStore()
	app.manager = initCampaignManager(app.queries, app.constants, app)
	app.messengers[emailMsgr] = initSMTPMessenger(app.manager)
	for _, m := range initPostbackMessengers(app.manager) {
		app.messengers[m.Name()] = m
	}
	for _, m := range app.messengers {
		app.manager.AddMessenger(m)
	}
	// Get and validate fields.
	if err := c.Bind(&req); err != nil {
		return err
	}

	// Validate.
	if c, err := validateCampaignFields(req, app); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	} else {
		req = c
	}
	if len(req.SubscriberEmails) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("campaigns.noSubsToTest"))
	}

	// Get the subscribers.
	for i := 0; i < len(req.SubscriberEmails); i++ {
		req.SubscriberEmails[i] = strings.ToLower(strings.TrimSpace(req.SubscriberEmails[i]))
	}

	subs, err := app.core.GetSubscribersByEmail(req.SubscriberEmails, authID)
	if err != nil {
		return err
	}

	// The campaign.
	camp, err := app.core.GetCampaignForPreview(campID, tplID, authID)
	if err != nil {
		return err
	}

	// Override certain values from the DB with incoming values.
	camp.Name = req.Name
	camp.Subject = req.Subject
	camp.FromEmail = req.FromEmail
	camp.Body = req.Body
	camp.AltBody = req.AltBody
	camp.Messenger = req.Messenger
	camp.ContentType = req.ContentType
	camp.Headers = req.Headers
	camp.TemplateID = req.TemplateID
	for _, id := range req.MediaIDs {
		if id > 0 {
			camp.MediaIDs = append(camp.MediaIDs, int64(id))
		}
	}

	// Send the test messages.
	for _, s := range subs {
		sub := s
		c := camp
		if err := sendTestMessage(sub, &c, app, authID); err != nil {
			app.log.Printf("error sending test message: %v", err)
			return echo.NewHTTPError(http.StatusInternalServerError,
				app.i18n.Ts("campaigns.errorSendTest", "error", err.Error()))
		}
	}

	return c.JSON(http.StatusOK, okResp{true})
}

// handleGetCampaignViewAnalytics retrieves view counts for a campaign.
func handleGetCampaignViewAnalytics(c echo.Context) error {
	var (
		app = c.Get("app").(*App)

		typ  = c.Param("type")
		from = c.QueryParams().Get("from")
		to   = c.QueryParams().Get("to")
	)
	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	ids, err := parseStringIDs(c.Request().URL.Query()["id"])
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest,
			app.i18n.Ts("globals.messages.errorInvalidIDs", "error", err.Error()))
	}

	if len(ids) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest,
			app.i18n.Ts("globals.messages.missingFields", "name", "`id`"))
	}

	if !strHasLen(from, 10, 30) || !strHasLen(to, 10, 30) {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("analytics.invalidDates"))
	}

	// Campaign link stats.
	if typ == "links" {
		out, err := app.core.GetCampaignAnalyticsLinks(ids, typ, from, to)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, okResp{out})
	}

	// View, click, bounce stats.
	out, err := app.core.GetCampaignAnalyticsCounts(ids, typ, from, to, authID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, okResp{out})
}

// sendTestMessage takes a campaign and a subscriber and sends out a sample campaign message.
func sendTestMessage(sub models.Subscriber, camp *models.Campaign, app *App, authID string) error {
	if err := camp.CompileTemplate(app.manager.TemplateFuncs(camp)); err != nil {
		app.log.Printf("error compiling template: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			app.i18n.Ts("templates.errorCompiling", "error", err.Error()))
	}

	// Create a sample campaign message.
	msg, err := app.manager.NewCampaignMessage(camp, sub)
	if err != nil {
		app.log.Printf("error rendering message: %v", err)
		return echo.NewHTTPError(http.StatusNotFound,
			app.i18n.Ts("templates.errorRendering", "error", err.Error()))
	}

	return app.manager.PushCampaignMessage(msg)
}

// validateCampaignFields validates incoming campaign field values.
func validateCampaignFields(c campaignReq, app *App) (campaignReq, error) {
	if c.FromEmail == "" {
		c.FromEmail = app.constants.FromEmail
	} else if !regexFromAddress.Match([]byte(c.FromEmail)) {
		if _, err := app.importer.SanitizeEmail(c.FromEmail); err != nil {
			return c, errors.New(app.i18n.T("campaigns.fieldInvalidFromEmail"))
		}
	}

	if !strHasLen(c.Name, 1, stdInputMaxLen) {
		return c, errors.New(app.i18n.T("campaigns.fieldInvalidName"))
	}

	// Larger char limit for subject as it can contain {{ go templating }} logic.
	if !strHasLen(c.Subject, 1, 5000) {
		return c, errors.New(app.i18n.T("campaigns.fieldInvalidSubject"))
	}

	// If there's a "send_at" date, it should be in the future.
	if c.SendAt.Valid {
		if c.SendAt.Time.Before(time.Now()) {
			return c, errors.New(app.i18n.T("campaigns.fieldInvalidSendAt"))
		}
	}

	if len(c.ListIDs) == 0 {
		return c, errors.New(app.i18n.T("campaigns.fieldInvalidListIDs"))
	}

	if !app.manager.HasMessenger(c.Messenger) {
		return c, errors.New(app.i18n.Ts("campaigns.fieldInvalidMessenger", "name", c.Messenger))
	}

	camp := models.Campaign{Body: c.Body, TemplateBody: tplTag}
	if err := c.CompileTemplate(app.manager.TemplateFuncs(&camp)); err != nil {
		return c, errors.New(app.i18n.Ts("campaigns.fieldInvalidBody", "error", err.Error()))
	}

	if len(c.Headers) == 0 {
		c.Headers = make([]map[string]string, 0)
	}

	if len(c.ArchiveMeta) == 0 {
		c.ArchiveMeta = json.RawMessage("{}")
	}

	if c.ArchiveSlug.String != "" {
		// Format the slug to be alpha-numeric-dash.
		s := strings.ToLower(c.ArchiveSlug.String)
		s = strings.TrimSpace(regexSlug.ReplaceAllString(s, " "))
		s = regexpSpaces.ReplaceAllString(s, "-")

		c.ArchiveSlug = null.NewString(s, true)
	} else {
		// If there's no slug set, set it to NULL in the DB.
		c.ArchiveSlug.Valid = false
	}

	return c, nil
}

// isCampaignalMutable tells if a campaign's in a state where it's
// properties can be mutated.
func isCampaignalMutable(status string) bool {
	return status == models.CampaignStatusRunning ||
		status == models.CampaignStatusCancelled ||
		status == models.CampaignStatusFinished
}

// makeOptinCampaignMessage makes a default opt-in campaign message body.
func makeOptinCampaignMessage(o campaignReq, app *App) (campaignReq, error) {
	if len(o.ListIDs) == 0 {
		return o, echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("campaigns.fieldInvalidListIDs"))
	}

	// Fetch double opt-in lists from the given list IDs.
	lists, err := app.core.GetListsByOptin(o.ListIDs, models.ListOptinDouble)
	if err != nil {
		return o, err
	}

	// No opt-in lists.
	if len(lists) == 0 {
		return o, echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("campaigns.noOptinLists"))
	}

	// Construct the opt-in URL with list IDs.
	listIDs := url.Values{}
	for _, l := range lists {
		listIDs.Add("l", l.UUID)
	}
	// optinURLFunc := template.URL("{{ OptinURL }}?" + listIDs.Encode())
	optinURLAttr := template.HTMLAttr(fmt.Sprintf(`href="{{ OptinURL }}%s"`, listIDs.Encode()))

	// Prepare sample opt-in message for the campaign.
	var b bytes.Buffer
	if err := app.notifTpls.tpls.ExecuteTemplate(&b, "optin-campaign", struct {
		Lists        []models.List
		OptinURLAttr template.HTMLAttr
	}{lists, optinURLAttr}); err != nil {
		app.log.Printf("error compiling 'optin-campaign' template: %v", err)
		return o, echo.NewHTTPError(http.StatusBadRequest,
			app.i18n.Ts("templates.errorCompiling", "error", err.Error()))
	}

	o.Body = b.String()
	return o, nil
}

// GetCampaignReport retrieves the campaign reports.
// If IDs are provided then, those specific campaign reports are returned, otherwise all campaign reports are returned.
func handleGetCampaignsReport(c echo.Context) error {
	var (
		app     = c.Get("app").(*App)
		orderBy = c.FormValue("order_by")
		order   = c.FormValue("order")
		status  = c.FormValue("status")
	)

	campaignIDs, err := getQueryInts("campaign_id", c.QueryParams())
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, app.i18n.T("globals.messages.invalidID"))
	}

	authID := c.Request().Header.Get("X-Auth-ID")
	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	campaigns, err := app.core.GetCampaignReport(campaignIDs, authID, order, orderBy, status)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, okResp{campaigns})
}
