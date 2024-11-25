package main

import (
	"fmt"
	"net/http"
	"sort"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
)

type serverConfig struct {
	Messengers   []string   `json:"messengers"`
	Langs        []i18nLang `json:"langs"`
	Lang         string     `json:"lang"`
	Update       *AppUpdate `json:"update"`
	NeedsRestart bool       `json:"needs_restart"`
	Version      string     `json:"version"`
}

// handleGetServerConfig returns general server config.
func handleGetServerConfig(c echo.Context) error {
	var (
		app = c.Get("app").(*App)
		out = serverConfig{}
	)
	// authID := c.Request().Header.Get("X-Auth-ID")

	// if authID == "" {
	// 	return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	// }

	// Language list.
	langList, err := getI18nLangList(app.constants.Lang, app)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError,
			fmt.Sprintf("Error loading language list: %v", err))
	}
	out.Langs = langList
	out.Lang = app.constants.Lang

	// Sort messenger names with `email` always as the first item.
	var names []string
	for name := range app.messengers {
		if name == emailMsgr {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	out.Messengers = append(out.Messengers, emailMsgr)
	out.Messengers = append(out.Messengers, names...)

	app.Lock()
	out.NeedsRestart = app.needsRestart
	out.Update = app.update
	app.Unlock()
	out.Version = versionString

	return c.JSON(http.StatusOK, okResp{out})
}

// handleGetDashboardCharts returns chart data points to render ont he dashboard.
func handleGetDashboardCharts(c echo.Context) error {
	var (
		app = c.Get("app").(*App)
	)

	out, err := app.core.GetDashboardCharts()
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, okResp{out})
}

// handleGetDashboardCounts returns stats counts to show on the dashboard.
func handleGetDashboardCounts(c echo.Context) error {
	var (
		app = c.Get("app").(*App)
	)
	authId := c.Request().Header.Get("X-Auth-ID")
	if authId == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	fromDate := c.QueryParam("from_date")
	toDate := c.QueryParam("to_date")
	if fromDate != "" || toDate != "" {
		RFC3339dateLayout := "2006-01-02"
		fromdate, err := time.Parse(RFC3339dateLayout, fromDate)

		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "Please validate from date"})
		}

		todate, err := time.Parse(RFC3339dateLayout, toDate)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "Please validate to date"})
		}

		now := time.Now()
		if fromdate.After(now) || todate.After(now) {
			return c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "Dates cannot be in the future"})
		}

		if fromdate.After(todate) {
			return c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "To date should be after the from date. Please validate from & to date"})
		}
	}

	out, err := app.core.GetDashboardCounts(authId, fromDate, toDate)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, okResp{out})
}

// handleReloadApp restarts the app.
func handleReloadApp(c echo.Context) error {

	// authID := c.Request().Header.Get("X-Auth-ID")

	// if authID == "" {
	// 	return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	// }
	app := c.Get("app").(*App)
	go func() {
		<-time.After(time.Millisecond * 500)
		app.chReload <- syscall.SIGHUP
	}()
	return c.JSON(http.StatusOK, okResp{true})
}
