package core

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/knadh/listmonk/internal/media"
	"github.com/knadh/listmonk/models"
	"github.com/labstack/echo/v4"
	"gopkg.in/volatiletech/null.v6"
)

// QueryMedia returns media entries optionally filtered by a query string.
func (c *Core) QueryMedia(provider string, s media.Store, query string, offset, limit int, authID string) ([]media.Media, int, error) {
	out := []media.Media{}

	if query != "" {
		query = strings.ToLower(query)
	}

	if err := c.q.QueryMedia.Select(&out, fmt.Sprintf("%%%s%%", query), provider, offset, limit, authID); err != nil {
		return out, 0, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching",
				"name", "{globals.terms.media}", "error", pqErrMsg(err)))
	}

	total := 0
	if len(out) > 0 {
		total = out[0].Total

		for i := 0; i < len(out); i++ {
			out[i].URL = s.GetURL(out[i].Filename)

			if out[i].Thumb != "" {
				out[i].ThumbURL = null.String{Valid: true, String: s.GetURL(out[i].Thumb)}
			}
		}
	}

	return out, total, nil
}

// GetMedia returns a media item.
func (c *Core) GetMedia(id int, uuid string, s media.Store, authID string) (media.Media, error) {
	var uu interface{}
	if uuid != "" {
		uu = uuid
	}

	var out media.Media
	if err := c.q.GetMedia.Get(&out, id, uu, authID); err != nil {
		if out.ID == 0 {
			return out, echo.NewHTTPError(http.StatusBadRequest,
				c.i18n.Ts("globals.messages.notFound", "name",
					fmt.Sprintf("{globals.terms.media} (%d:)", id)))
		}
		return out, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.media}", "error", pqErrMsg(err)))
	}

	out.URL = s.GetURL(out.Filename)
	if out.Thumb != "" {
		out.ThumbURL = null.String{Valid: true, String: s.GetURL(out.Thumb)}
	}

	return out, nil
}

// InsertMedia inserts a new media file into the DB.
func (c *Core) InsertMedia(fileName, thumbName, contentType string, meta models.JSON, provider string, s media.Store, authID string) (media.Media, error) {
	uu, err := uuid.NewV4()
	if err != nil {
		c.log.Printf("error generating UUID: %v", err)
		return media.Media{}, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorUUID", "error", err.Error()))
	}

	// Write to the DB.
	var newID int
	if err := c.q.InsertMedia.Get(&newID, uu, fileName, thumbName, contentType, provider, meta, authID); err != nil {
		c.log.Printf("error inserting uploaded file to db: %v", err)
		return media.Media{}, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorCreating", "name", "{globals.terms.media}", "error", pqErrMsg(err)))
	}

	return c.GetMedia(newID, "", s, authID)
}

// DeleteMedia deletes a given media item and returns the filename of the deleted item.
func (c *Core) DeleteMedia(id int, authID string) error {
	res, err := c.q.DeleteMedia.Exec(id, authID)
	if err != nil {
		c.log.Printf("error deleting media: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorDeleting", "name", "{globals.terms.media}", "error", pqErrMsg(err)))
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return echo.NewHTTPError(http.StatusBadRequest,
			c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.media}"))
	}

	return nil
}
func (c *Core) GetExtensions(authID string) ([]string, error) {

	var out []string
	if err := c.q.GetExtensions.Select(&out, authID); err != nil {
		return nil, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.media}", "error", pqErrMsg(err)))
	}

	return out, nil
}
func (c *Core) GetFilePath(authID string) (string, error) {

	var out string
	if err := c.q.GetFilePath.Get(&out, authID); err != nil {
		if len(out) == 0 {
			return "", echo.NewHTTPError(http.StatusBadRequest,
				c.i18n.Ts("globals.messages.notFound", "name",
					fmt.Sprintf("{globals.terms.media}")))
		}
		return "", echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.media}", "error", pqErrMsg(err)))
	}

	return out, nil
}
func (c *Core) GetUploadProvider(authID string) (string, error) {

	var upload_provider string
	if err := c.q.GetUploadProvider.Get(&upload_provider, authID); err != nil {
		if len(upload_provider) == 0 {
			return "", echo.NewHTTPError(http.StatusBadRequest,
				c.i18n.Ts("globals.messages.notFound", "name", "{globals.terms.media}"))
		}
		return "", echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.media}", "error", pqErrMsg(err)))
	}
	upload_provider = strings.Trim(upload_provider, "\"")
	upload_provider = strings.TrimSpace(upload_provider)

	return upload_provider, nil
}

func (c *Core) GetS3UploadData(authID string) (map[string]interface{}, error) {
	opts := make(map[string]interface{})

	var rows []struct {
		Key   string `db:"key"`
		Value string `db:"value"`
	}

	if err := c.q.GetS3UploadData.Select(&rows, authID); err != nil {
		return nil, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.media}", "error", pqErrMsg(err)))
	}

	for _, row := range rows {
		// Trim surrounding quotes if they exist
		trimmedValue := strings.Trim(row.Value, "\"")

		switch row.Key {
		case "upload.s3.url":
			opts["url"] = trimmedValue
		case "upload.s3.public_url":
			opts["public_url"] = trimmedValue
		case "upload.s3.aws_access_key_id":
			opts["aws_access_key_id"] = trimmedValue
		case "upload.s3.aws_secret_access_key":
			opts["aws_secret_access_key"] = trimmedValue
		case "upload.s3.aws_default_region":
			opts["aws_default_region"] = trimmedValue
		case "upload.s3.bucket":
			opts["bucket"] = trimmedValue
		case "upload.s3.bucket_path":
			opts["bucket_path"] = trimmedValue
		case "upload.s3.bucket_type":
			opts["bucket_type"] = trimmedValue
		case "upload.s3.expiry":
			if expiry, err := time.ParseDuration(row.Value); err == nil {
				opts["expiry"] = expiry
			} else {
				opts["expiry"] = row.Value
			}
		}
	}

	return opts, nil
}

func (c *Core) GetFileSystemUploadData(authID string) (map[string]interface{}, error) {
	opts := make(map[string]interface{})

	var rows []struct {
		Key   string `db:"key"`
		Value string `db:"value"`
	}

	// Execute the query and fetch all matching rows
	err := c.q.GetFileSystemUploadData.Select(&rows, authID)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusInternalServerError,
			c.i18n.Ts("globals.messages.errorFetching", "name", "{globals.terms.media}", "error", pqErrMsg(err)))
	}

	// Iterate over the rows and map them to the appropriate keys in the 'opts' map
	for _, row := range rows {
		trimmedValue := strings.Trim(row.Value, "\"")
		switch row.Key {
		case "upload.filesystem.upload_path":
			opts["upload_path"] = trimmedValue
		case "upload.filesystem.upload_uri":
			opts["upload_uri"] = trimmedValue
		case "app.root_url":
			opts["root_url"] = trimmedValue
		}
	}

	return opts, nil
}
