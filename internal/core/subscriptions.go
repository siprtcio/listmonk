package core

import (
	"net/http"
	"time"

	"github.com/knadh/listmonk/models"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
)

// GetSubscriptions retrieves the subscriptions for a subscriber.
func (c *Core) GetSubscriptions(subID int, subUUID string, allLists bool, authID string) ([]models.Subscription, error) {
	var out []models.Subscription
	err := c.q.GetSubscriptions.Select(&out, subID, subUUID, allLists, authID)
	if err != nil {
		c.log.Printf("error getting subscriptions: %v", err)
		return nil, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", err.Error()))
	}

	return out, err
}

// AddSubscriptions adds list subscriptions to subscribers.
func (c *Core) AddSubscriptions(subIDs, listIDs []int, status string, authID string) error {
	var listCount, subscriberCount int
	if err := c.q.CheckListsByAuthID.Get(&listCount, authID, pq.Array(listIDs)); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.lists}", "error", pqErrMsg(err)))
	}
	if listCount != len(listIDs) {
		return echo.NewHTTPError(http.StatusBadRequest, c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.list}"))
	}

	if err := c.q.CheckSubscribersByAuthID.Get(&subscriberCount, authID, pq.Array(subIDs)); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	if subscriberCount != len(subIDs) {
		return echo.NewHTTPError(http.StatusBadRequest, c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.subscribers}"))
	}

	if _, err := c.q.AddSubscribersToLists.Exec(pq.Array(subIDs), pq.Array(listIDs), status, authID); err != nil {
		c.log.Printf("error adding subscriptions: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscriptions}", "error", err.Error()))
	}

	return nil
}

// AddSubscriptionsByQuery adds list subscriptions to subscribers by a given arbitrary query expression.
// sourceListIDs is the list of list IDs to filter the subscriber query with.
func (c *Core) AddSubscriptionsByQuery(query string, sourceListIDs, targetListIDs []int, status string, subStatus string, authID string) error {
	if sourceListIDs == nil {
		sourceListIDs = []int{}
	}

	err := c.q.ExecSubQueryTpl(sanitizeSQLExp(query), c.q.AddSubscribersToListsByQuery, sourceListIDs, c.db, subStatus, pq.Array(targetListIDs), status, authID)
	if err != nil {
		c.log.Printf("error adding subscriptions by query: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	return nil
}

// DeleteSubscriptions delete list subscriptions from subscribers.
func (c *Core) DeleteSubscriptions(subIDs, listIDs []int, authID string) error {
	var listCount, subscriberCount int
	if err := c.q.CheckListsByAuthID.Get(&listCount, authID, pq.Array(listIDs)); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.lists}", "error", pqErrMsg(err)))
	}
	if listCount != len(listIDs) {
		return echo.NewHTTPError(http.StatusBadRequest, c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.list}"))
	}

	if err := c.q.CheckSubscribersByAuthID.Get(&subscriberCount, authID, pq.Array(subIDs)); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	if subscriberCount != len(subIDs) {
		return echo.NewHTTPError(http.StatusBadRequest, c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.subscribers}"))
	}

	res, err := c.q.DeleteSubscriptions.Exec(pq.Array(subIDs), pq.Array(listIDs), authID)
	if err != nil {
		c.log.Printf("error deleting subscriptions: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscriptions}", "error", err.Error()))
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return echo.NewHTTPError(http.StatusBadRequest,
			c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.subscriptions}"))
	}

	return nil
}

// DeleteSubscriptionsByQuery deletes list subscriptions from subscribers by a given arbitrary query expression.
// sourceListIDs is the list of list IDs to filter the subscriber query with.
func (c *Core) DeleteSubscriptionsByQuery(query string, sourceListIDs, targetListIDs []int, subStatus string, authID string) error {
	if sourceListIDs == nil {
		sourceListIDs = []int{}
	}

	err := c.q.ExecSubQueryTpl(sanitizeSQLExp(query), c.q.DeleteSubscriptionsByQuery, sourceListIDs, c.db, subStatus, pq.Array(targetListIDs), authID)
	if err != nil {
		c.log.Printf("error deleting subscriptions by query: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	return nil
}

// UnsubscribeLists sets list subscriptions to 'unsubscribed'.
func (c *Core) UnsubscribeLists(subIDs, listIDs []int, listUUIDs []string, authID string) error {
	var listCount, subscriberCount int
	if err := c.q.CheckListsByAuthID.Get(&listCount, authID, pq.Array(listIDs)); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.lists}", "error", pqErrMsg(err)))
	}
	if listCount != len(listIDs) {
		return echo.NewHTTPError(http.StatusBadRequest, c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.list}"))
	}

	if err := c.q.CheckSubscribersByAuthID.Get(&subscriberCount, authID, pq.Array(subIDs)); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	if subscriberCount != len(subIDs) {
		return echo.NewHTTPError(http.StatusBadRequest, c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.subscribers}"))
	}

	res, err := c.q.UnsubscribeSubscribersFromLists.Exec(pq.Array(subIDs), pq.Array(listIDs), pq.StringArray(listUUIDs), authID)
	if err != nil {
		c.log.Printf("error unsubscribing from lists: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscriptions}", "error", err.Error()))
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return echo.NewHTTPError(http.StatusBadRequest,
			c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.subscriptions}"))
	}

	return nil
}

// UnsubscribeListsByQuery sets list subscriptions to 'unsubscribed' by a given arbitrary query expression.
// sourceListIDs is the list of list IDs to filter the subscriber query with.
func (c *Core) UnsubscribeListsByQuery(query string, sourceListIDs, targetListIDs []int, subStatus string, authID string) error {
	if sourceListIDs == nil {
		sourceListIDs = []int{}
	}

	err := c.q.ExecSubQueryTpl(sanitizeSQLExp(query), c.q.UnsubscribeSubscribersFromListsByQuery, sourceListIDs, c.db, subStatus, pq.Array(targetListIDs), authID)
	if err != nil {
		c.log.Printf("error unsubscribing from lists by query: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUpdating", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	return nil
}

// DeleteUnconfirmedSubscriptions sets list subscriptions to 'unsubscribed' by a given arbitrary query expression.
// sourceListIDs is the list of list IDs to filter the subscriber query with.
func (c *Core) DeleteUnconfirmedSubscriptions(beforeDate time.Time, authID string) (int, error) {
	res, err := c.q.DeleteUnconfirmedSubscriptions.Exec(beforeDate, authID)
	if err != nil {
		c.log.Printf("error deleting unconfirmed subscribers: %v", err)
		return 0, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorDeleting", "name", "{globals.terms.subscribers}", "error", pqErrMsg(err)))
	}

	n, _ := res.RowsAffected()
	return int(n), nil
}
