package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gofrs/uuid/v5"
	"github.com/knadh/listmonk/models"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
)

// GetSubscriber fetches a subscriber by one of the given params.
func (c *Core) GetSubscriber(id int, uuid, email string, authID string) (models.Subscriber, error) {
	var uu interface{}
	if uuid != "" {
		uu = uuid
	}

	var out models.Subscribers
	if err := c.q.GetSubscriber.Select(&out, id, uu, email, authID); err != nil {
		c.log.Printf("error fetching subscriber: %v", err)
		return models.Subscriber{}, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching",
				"name", "{globals.terms.subscriber}", "error", pqErrMsg(err)))
	}
	if len(out) == 0 {
		return models.Subscriber{}, echo.NewHTTPError(http.StatusBadRequest,
			c.i18n.Ts("globals.messages.notFound", "name",
				fmt.Sprintf("{globals.terms.subscriber} (%d: %s%s)", id, uuid, email)))
	}
	if err := out.LoadLists(c.q.GetSubscriberListsLazy); err != nil {
		c.log.Printf("error loading subscriber lists: %v", err)
		return models.Subscriber{}, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching",
				"name", "{globals.terms.lists}", "error", pqErrMsg(err)))
	}

	return out[0], nil
}

func (c *Core) GetSubscribersByAuthID(authID string) ([]models.Subscriber, error) {
	// Create a slice to hold the subscribers
	var out []models.Subscriber

	// Query the database to get subscribers associated with the given authid
	if err := c.q.GetSubscribersByAuthID.Select(&out, authID); err != nil {
		c.log.Printf("error fetching subscribers for authid %s: %v", authID, err)
		return nil, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching",
				"name", "{globals.terms.subscriber}", "error", pqErrMsg(err)))
	}

	// Log the number of subscribers found
	c.log.Printf("Number of subscribers found for authid %s: %d", authID, len(out))

	// Check if any subscribers were found
	if len(out) == 0 {
		return nil, echo.NewHTTPError(http.StatusNotFound,
			c.i18n.Ts("globals.messages.notFound", "name",
				fmt.Sprintf("{globals.terms.subscriber} for authid %s", authID)))
	}

	return out, nil
}

// GetSubscribersByEmail fetches a subscriber by one of the given params.
func (c *Core) GetSubscribersByEmail(emails []string, authID string) (models.Subscribers, error) {
	var out models.Subscribers

	if err := c.q.GetSubscribersByEmails.Select(&out, pq.Array(emails), authID); err != nil {
		c.log.Printf("error fetching subscriber: %v", err)
		return nil, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscriber}", "error", pqErrMsg(err)))
	}
	if len(out) == 0 {
		return nil, echo.NewHTTPError(http.StatusBadRequest, c.i18n.T("campaigns.noKnownSubsToTest"))
	}

	if err := out.LoadLists(c.q.GetSubscriberListsLazy); err != nil {
		c.log.Printf("error loading subscriber lists: %v", err)
		return nil, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.lists}", "error", pqErrMsg(err)))
	}

	return out, nil
}

// QuerySubscribers queries and returns paginated subscrribers based on the given params including the total count.
func (c *Core) QuerySubscribers(query string, listIDs []int, subStatus string, order, orderBy string, offset, limit int, authID string) (models.Subscribers, int, error) {
	// There's an arbitrary query condition.
	cond := ""
	if query != "" {
		cond = " AND " + query
	}

	// Sort params.
	if !strSliceContains(orderBy, subQuerySortFields) {
		orderBy = "subscribers.id"
	}
	if order != SortAsc && order != SortDesc {
		order = SortDesc
	}

	// Required for pq.Array()
	if listIDs == nil {
		listIDs = []int{}
	}

	// Create a readonly transaction that just does COUNT() to obtain the count of results
	// and to ensure that the arbitrary query is indeed readonly.
	total, err := c.getSubscriberCount(cond, subStatus, listIDs, authID)
	if err != nil {
		return nil, 0, err
	}

	// No results.
	if total == 0 {
		return models.Subscribers{}, 0, nil
	}

	// Run the query again and fetch the actual data. stmt is the raw SQL query.
	var out models.Subscribers
	stmt := fmt.Sprintf(c.q.QuerySubscribersCount, cond)
	stmt = strings.ReplaceAll(c.q.QuerySubscribers, "%query%", cond)
	stmt = strings.ReplaceAll(stmt, "%order%", orderBy+" "+order)

	tx, err := c.db.BeginTxx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		c.log.Printf("error preparing subscriber query: %v", err)
		return nil, 0, echo.NewHTTPError(http.StatusBadRequest, c.i18n.Ts("subscribers.errorPreparingQuery", "error", pqErrMsg(err)))
	}
	defer tx.Rollback()

	if err := tx.Select(&out, stmt, pq.Array(listIDs), subStatus, offset, limit, authID); err != nil {
		return nil, 0, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	// Lazy load lists for each subscriber.
	if err := out.LoadLists(c.q.GetSubscriberListsLazy); err != nil {
		c.log.Printf("error fetching subscriber lists: %v", err)
		return nil, 0, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	return out, total, nil
}

// GetSubscriberLists returns a subscriber's lists based on the given conditions.
func (c *Core) GetSubscriberLists(subID int, uuid string, listIDs []int, listUUIDs []string, subStatus string, listType string, authID string) ([]models.List, error) {
	if listIDs == nil {
		listIDs = []int{}
	}
	if listUUIDs == nil {
		listUUIDs = []string{}
	}

	var uu interface{}
	if uuid != "" {
		uu = uuid
	}

	// Fetch double opt-in lists from the given list IDs.
	// Get the list of subscription lists where the subscriber hasn't confirmed.
	out := []models.List{}
	if err := c.q.GetSubscriberLists.Select(&out, subID, uu, pq.Array(listIDs), pq.Array(listUUIDs), subStatus, listType, authID); err != nil {
		c.log.Printf("error fetching lists for opt-in: %s", pqErrMsg(err))
		return nil, err
	}

	return out, nil
}

// GetSubscriberProfileForExport returns the subscriber's profile data as a JSON exportable.
// Get the subscriber's data. A single query that gets the profile, list subscriptions, campaign views,
// and link clicks. Names of private lists are replaced with "Private list".
func (c *Core) GetSubscriberProfileForExport(id int, uuid string, authID string) (models.SubscriberExportProfile, error) {
	var uu interface{}
	if uuid != "" {
		uu = uuid
	}

	var out models.SubscriberExportProfile
	if err := c.q.ExportSubscriberData.Get(&out, id, uu, authID); err != nil {
		c.log.Printf("error fetching subscriber export data: %v", err)

		return models.SubscriberExportProfile{}, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", err.Error()))
	}
	if out.Email == "" {
		return models.SubscriberExportProfile{}, echo.NewHTTPError(http.StatusNotFound,
			c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.subscribers}"))
	}

	return out, nil
}

// ExportSubscribers returns an iterator function that provides lists of subscribers based
// on the given criteria in an exportable form. The iterator function returned can be called
// repeatedly until there are nil subscribers. It's an iterator because exports can be extremely
// large and may have to be fetched in batches from the DB and streamed somewhere.
func (c *Core) ExportSubscribers(query string, subIDs, listIDs []int, subStatus string, batchSize int, authID string) (func() ([]models.SubscriberExport, error), error) {
	// There's an arbitrary query condition.
	cond := ""
	if query != "" {
		cond = " AND " + query
	}

	stmt := fmt.Sprintf(c.q.QuerySubscribersForExport, cond)
	stmt = strings.ReplaceAll(c.q.QuerySubscribersForExport, "%query%", cond)

	// Verify that the arbitrary SQL search expression is read only.
	if cond != "" {
		tx, err := c.db.Unsafe().BeginTxx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			c.log.Printf("error preparing subscriber query: %v", err)
			return nil, echo.NewHTTPError(http.StatusBadRequest,
				c.i18n.Ts("subscribers.errorPreparingQuery", "error", pqErrMsg(err)))
		}
		defer tx.Rollback()

		if _, err := tx.Query(stmt, nil, 0, nil, 1); err != nil {
			return nil, echo.NewHTTPError(http.StatusBadRequest,
				c.i18n.Ts("subscribers.errorPreparingQuery", "error", pqErrMsg(err)))
		}
	}

	if subIDs == nil {
		subIDs = []int{}
	}
	if listIDs == nil {
		listIDs = []int{}
	}

	// Prepare the actual query statement.
	tx, err := c.db.Preparex(stmt)
	if err != nil {
		c.log.Printf("error preparing subscriber query: %v", err)
		return nil, echo.NewHTTPError(http.StatusBadRequest,
			c.i18n.Ts("subscribers.errorPreparingQuery", "error", pqErrMsg(err)))
	}

	id := 0
	return func() ([]models.SubscriberExport, error) {
		var out []models.SubscriberExport
		if err := tx.Select(&out, pq.Array(listIDs), id, pq.Array(subIDs), subStatus, batchSize, authID); err != nil {
			c.log.Printf("error exporting subscribers by query: %v", err)
			return nil, echo.NewHTTPError(http.StatusInternalServerError,
				c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
		}
		if len(out) == 0 {
			return nil, nil
		}

		id = out[len(out)-1].ID
		return out, nil
	}, nil
}

// InsertSubscriber inserts a subscriber and returns the ID. The first bool indicates if
// it was a new subscriber, and the second bool indicates if the subscriber was sent an optin confirmation.
// bool = optinSent?
func (c *Core) InsertSubscriber(sub models.Subscriber, listIDs []int, listUUIDs []string, preconfirm bool, authID string) (models.Subscriber, bool, error) {
	uu, err := uuid.NewV4()
	if err != nil {
		c.log.Printf("error generating UUID: %v", err)
		return models.Subscriber{}, false, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUUID", "error", err.Error()))
	}
	sub.UUID = uu.String()

	sub.AuthID = authID

	subStatus := models.SubscriptionStatusUnconfirmed
	if preconfirm {
		subStatus = models.SubscriptionStatusConfirmed
	}
	if sub.Status == "" {
		sub.Status = models.UserStatusEnabled
	}

	// For pq.Array()
	if listIDs == nil {
		listIDs = []int{}
	}
	if listUUIDs == nil {
		listUUIDs = []string{}
	}

	if err = c.q.InsertSubscriber.Get(&sub.ID,
		sub.UUID,
		sub.Email,
		strings.TrimSpace(sub.Name),
		sub.Status,
		sub.Attribs,
		pq.Array(listIDs),
		pq.Array(listUUIDs),
		subStatus,
		sub.AuthID); err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Constraint == "idx_subs_email_authid" {
			return models.Subscriber{}, false, echo.NewHTTPError(http.StatusConflict, c.i18n.T("subscribers.emailExists"))
		} else {
			// return sub.Subscriber, errSubscriberExists
			c.log.Printf("error inserting subscriber: %v", err)
			return models.Subscriber{}, false, echo.NewHTTPError(http.StatusInternalServerError,
				c.i18n.Ts("globals.messages.errorCreating", "name", "{globals.terms.subscriber}", "error", pqErrMsg(err)))
		}
	}

	// Fetch the subscriber's full data. If the subscriber already existed and wasn't
	// created, the id will be empty. Fetch the details by e-mail then.
	out, err := c.GetSubscriber(sub.ID, "", sub.Email, sub.AuthID)
	if err != nil {
		return models.Subscriber{}, false, err
	}

	hasOptin := false
	if !preconfirm && c.consts.SendOptinConfirmation {
		// Send a confirmation e-mail (if there are any double opt-in lists).
		num, _ := c.h.SendOptinConfirmation(out, listIDs)
		hasOptin = num > 0
	}

	return out, hasOptin, nil
}

// UpdateSubscriber updates a subscriber's properties.
func (c *Core) UpdateSubscriber(id int, sub models.Subscriber, authID string) (models.Subscriber, error) {
	// Format raw JSON attributes.
	attribs := []byte("{}")
	if len(sub.Attribs) > 0 {
		if b, err := json.Marshal(sub.Attribs); err != nil {
			return models.Subscriber{}, echo.NewHTTPError(http.StatusInternalServerError,
				c.i18n.Ts("globals.messages.errorUpdating",
					"name", "{globals.terms.subscriber}", "error", err.Error()))
		} else {
			attribs = b
		}
	}

	sub.AuthID = authID

	_, err := c.q.UpdateSubscriber.Exec(id,
		sub.Email,
		strings.TrimSpace(sub.Name),
		sub.Status,
		json.RawMessage(attribs),
		sub.AuthID,
	)
	if err != nil {
		c.log.Printf("error updating subscriber: %v", err)
		return models.Subscriber{}, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscriber}", "error", pqErrMsg(err)))
	}

	out, err := c.GetSubscriber(sub.ID, "", sub.Email, sub.AuthID)
	if err != nil {
		return models.Subscriber{}, err
	}

	return out, nil
}

// UpdateSubscriberWithLists updates a subscriber's properties.
// If deleteLists is set to true, all existing subscriptions are deleted and only
// the ones provided are added or retained.
func (c *Core) UpdateSubscriberWithLists(id int, sub models.Subscriber, listIDs []int, listUUIDs []string, preconfirm, deleteLists bool, authID string) (models.Subscriber, bool, error) {
	subStatus := models.SubscriptionStatusUnconfirmed
	if preconfirm {
		subStatus = models.SubscriptionStatusConfirmed
	}

	// Format raw JSON attributes.
	attribs := []byte("{}")
	if len(sub.Attribs) > 0 {
		if b, err := json.Marshal(sub.Attribs); err != nil {
			return models.Subscriber{}, false, echo.NewHTTPError(http.StatusInternalServerError,
				c.i18n.Ts("globals.messages.errorUpdating",
					"name", "{globals.terms.subscriber}", "error", err.Error()))
		} else {
			attribs = b
		}
	}
	sub.AuthID = authID

	_, err := c.q.UpdateSubscriberWithLists.Exec(id,
		sub.Email,
		strings.TrimSpace(sub.Name),
		sub.Status,
		json.RawMessage(attribs),
		pq.Array(listIDs),
		pq.Array(listUUIDs),
		subStatus,
		deleteLists,
		sub.AuthID)
	if err != nil {
		c.log.Printf("error updating subscriber: %v", err)
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Constraint == "subscribers_email_key" {
			return models.Subscriber{}, false, echo.NewHTTPError(http.StatusConflict, c.i18n.T("subscribers.emailExists"))
		} else {
			c.log.Printf("error updating subscriber: %v", err)
			return models.Subscriber{}, false, echo.NewHTTPError(http.StatusInternalServerError,
				c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscriber}", "error", pqErrMsg(err)))
		}
	}

	out, err := c.GetSubscriber(sub.ID, "", sub.Email, sub.AuthID)
	if err != nil {
		return models.Subscriber{}, false, err
	}

	hasOptin := false
	if !preconfirm && c.consts.SendOptinConfirmation {
		// Send a confirmation e-mail (if there are any double opt-in lists).
		num, _ := c.h.SendOptinConfirmation(out, listIDs)
		hasOptin = num > 0
	}

	return out, hasOptin, nil
}

// BlocklistSubscribers blocklists the given list of subscribers.
func (c *Core) BlocklistSubscribers(subIDs []int, authID string) error {
	res, err := c.q.BlocklistSubscribers.Exec(pq.Array(subIDs), authID)

	if err != nil {
		c.log.Printf("error blocklisting subscribers: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("subscribers.errorBlocklisting", "error", err.Error()))
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return echo.NewHTTPError(http.StatusBadRequest,
			c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.subscribers}"))
	}

	return nil
}

// BlocklistSubscribersByQuery blocklists the given list of subscribers.
func (c *Core) BlocklistSubscribersByQuery(query string, listIDs []int, subStatus string, authID string) error {
	if err := c.q.ExecSubQueryTpl(sanitizeSQLExp(query), c.q.BlocklistSubscribersByQuery, listIDs, c.db, subStatus, authID); err != nil {
		c.log.Printf("error blocklisting subscribers: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("subscribers.errorBlocklisting", "error", pqErrMsg(err)))
	}

	return nil
}

// DeleteSubscribers deletes the given list of subscribers.
func (c *Core) DeleteSubscribers(subIDs []int, subUUIDs []string, authID string) error {
	if subIDs == nil {
		subIDs = []int{}
	}
	if subUUIDs == nil {
		subUUIDs = []string{}
	}

	res, err := c.q.DeleteSubscribers.Exec(pq.Array(subIDs), pq.Array(subUUIDs), authID)

	if err != nil {
		c.log.Printf("error deleting subscribers: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorDeleting", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return echo.NewHTTPError(http.StatusBadRequest,
			c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.subscribers}"))
	}

	return nil
}

// DeleteSubscribersByQuery deletes subscribers by a given arbitrary query expression.
func (c *Core) DeleteSubscribersByQuery(query string, listIDs []int, subStatus string, authID string) error {
	err := c.q.ExecSubQueryTpl(sanitizeSQLExp(query), c.q.DeleteSubscribersByQuery, listIDs, c.db, subStatus, authID)
	if err != nil {
		c.log.Printf("error deleting subscribers: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorDeleting", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	return err
}

// UnsubscribeByCampaign unsubscribes a given subscriber from lists in a given campaign.
func (c *Core) UnsubscribeByCampaign(subUUID, campUUID string, blocklist bool, authID string) error {
	if _, err := c.q.UnsubscribeByCampaign.Exec(campUUID, subUUID, blocklist, authID); err != nil {
		c.log.Printf("error unsubscribing: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	return nil
}

// ConfirmOptionSubscription confirms a subscriber's optin subscription.
func (c *Core) ConfirmOptionSubscription(subUUID string, listUUIDs []string, meta models.JSON, authID string) error {
	if meta == nil {
		meta = models.JSON{}
	}

	if _, err := c.q.ConfirmSubscriptionOptin.Exec(subUUID, pq.Array(listUUIDs), meta, authID); err != nil {
		c.log.Printf("error confirming subscription: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	return nil
}

// DeleteSubscriberBounces deletes the given list of subscribers.
func (c *Core) DeleteSubscriberBounces(id int, uuid string, authID string) error {
	var uu interface{}
	if uuid != "" {
		uu = uuid
	}

	if _, err := c.q.DeleteBouncesBySubscriber.Exec(id, uu, authID); err != nil {
		c.log.Printf("error deleting bounces: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorDeleting", "name", "{globals.terms.bounces}", "error", pqErrMsg(err)))
	}

	return nil
}

// DeleteOrphanSubscribers deletes orphan subscriber records (subscribers without lists).
func (c *Core) DeleteOrphanSubscribers(authID string) (int, error) {
	res, err := c.q.DeleteOrphanSubscribers.Exec(authID)
	if err != nil {
		c.log.Printf("error deleting orphan subscribers: %v", err)
		return 0, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorDeleting", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	n, _ := res.RowsAffected()
	return int(n), nil
}

// DeleteBlocklistedSubscribers deletes blocklisted subscribers.
func (c *Core) DeleteBlocklistedSubscribers(authID string) (int, error) {
	res, err := c.q.DeleteBlocklistedSubscribers.Exec(authID)
	if err != nil {
		c.log.Printf("error deleting blocklisted subscribers: %v", err)
		return 0, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorDeleting", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	n, _ := res.RowsAffected()
	return int(n), nil
}

func (c *Core) getSubscriberCount(cond, subStatus string, listIDs []int, authId string) (int, error) {
	// If there's no condition, it's a "get all" call which can probably be optionally pulled from cache.
	if cond == "" {
		_ = c.refreshCache(matListSubStats, false)

		total := 0
		if err := c.q.QuerySubscribersCountAll.Get(&total, pq.Array(listIDs), subStatus, authId); err != nil {
			return 0, echo.NewHTTPError(http.StatusInternalServerError,
				c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
		}

		return total, nil
	}

	// Create a readonly transaction that just does COUNT() to obtain the count of results
	// and to ensure that the arbitrary query is indeed readonly.
	stmt := fmt.Sprintf(c.q.QuerySubscribersCount, cond)
	tx, err := c.db.BeginTxx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		c.log.Printf("error preparing subscriber query: %v", err)
		return 0, echo.NewHTTPError(http.StatusBadRequest, c.i18n.Ts("subscribers.errorPreparingQuery", "error", pqErrMsg(err)))
	}
	defer tx.Rollback()

	// Execute the readonly query and get the count of results.
	total := 0
	if err := tx.Get(&total, stmt, pq.Array(listIDs), subStatus, authId); err != nil {
		return 0, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	return total, nil
}
