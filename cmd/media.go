package main

import (
	"bytes"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/disintegration/imaging"
	"github.com/knadh/listmonk/models"
	"github.com/labstack/echo/v4"
)

const (
	thumbPrefix   = "thumb_"
	thumbnailSize = 250
)

var (
	vectorExts = []string{"svg"}
	imageExts  = []string{"gif", "png", "jpg", "jpeg"}
)

// handleUploadMedia handles media file uploads.
func handleUploadMedia(c echo.Context) error {
	var (
		app     = c.Get("app").(*App)
		cleanUp = false
	)

	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}

	file, err := c.FormFile("file")
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest,
			app.i18n.Ts("media.invalidFile", "error", err.Error()))
	}

	// Read file contents in memory
	src, err := file.Open()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError,
			app.i18n.Ts("media.errorReadingFile", "error", err.Error()))
	}
	defer src.Close()

	var (
		// Naive check for content type and extension.
		ext         = strings.TrimPrefix(strings.ToLower(filepath.Ext(file.Filename)), ".")
		contentType = file.Header.Get("Content-Type")
	)
	if !isASCII(file.Filename) {
		return echo.NewHTTPError(http.StatusUnprocessableEntity,
			app.i18n.Ts("media.invalidFileName", "name", file.Filename))
	}

	uploadProvider, err := app.core.GetUploadProvider(authID)
	if err != nil {
		return err
	}
	var uploadData map[string]interface{}
	if uploadProvider == "filesystem" {
		uploadData, err = app.core.GetFileSystemUploadData(authID)
		if err != nil {
			return err
		}
	} else {
		uploadData, err = app.core.GetS3UploadData(authID)
		if err != nil {
			return err
		}
	}

	initSettings("SELECT JSON_OBJECT_AGG(key, value) AS settings FROM settings WHERE authid = $1;", db, ko, authID)
	app.media = initMediaStore(uploadProvider, uploadData)
	if app.media == nil {
		log.Println("app.media is nil during initialization")
	}

	app.manager = initCampaignManager(app.queries, app.constants, app)
	app.messengers[emailMsgr] = initSMTPMessenger(app.manager)
	for _, m := range initPostbackMessengers(app.manager) {
		app.messengers[m.Name()] = m
	}
	for _, m := range app.messengers {
		app.manager.AddMessenger(m)
	}

	extensions, err := app.core.GetExtensions(authID)
	if err != nil {
		cleanUp = true
		return err
	}

	// Validate file extension.
	if !inArray("*", extensions) {
		ok := inArray(ext, extensions)
		if !ok {
			return echo.NewHTTPError(http.StatusBadRequest,
				app.i18n.Ts("media.unsupportedFileType", "type", ext))
		}
	}

	// Sanitize filename.
	fName := makeFilename(file.Filename)

	// Add a random suffix to the filename to ensure uniqueness.
	suffix, _ := generateRandomString(6)
	fName = appendSuffixToFilename(fName, suffix)

	filePath, err := app.core.GetFilePath(authID)
	if err != nil {
		cleanUp = true
		return err
	}
	filePath = strings.Trim(filePath, "\"")
	filePath = strings.TrimSpace(filePath)

	// Upload the file.
	fName, err = app.media.Put(fName, contentType, src, filePath)
	if err != nil {
		app.log.Printf("error uploading file: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError,
			app.i18n.Ts("media.errorUploading", "error", err.Error()))
	}

	var (
		thumbfName = ""
		width      = 0
		height     = 0
	)
	defer func() {
		// If any of the subroutines in this function fail,
		// the uploaded image should be removed.
		if cleanUp {
			app.media.Delete(fName)

			if thumbfName != "" {
				app.media.Delete(thumbfName)
			}
		}
	}()

	// Create thumbnail from file for non-vector formats.
	isImage := inArray(ext, imageExts)
	if isImage {
		thumbFile, w, h, err := processImage(file)
		if err != nil {
			cleanUp = true
			app.log.Printf("error resizing image: %v", err)
			return echo.NewHTTPError(http.StatusInternalServerError,
				app.i18n.Ts("media.errorResizing", "error", err.Error()))
		}
		width = w
		height = h

		// Upload thumbnail.
		tf, err := app.media.Put(thumbPrefix+fName, contentType, thumbFile, filePath)
		if err != nil {
			cleanUp = true
			app.log.Printf("error saving thumbnail: %v", err)
			return echo.NewHTTPError(http.StatusInternalServerError,
				app.i18n.Ts("media.errorSavingThumbnail", "error", err.Error()))
		}
		thumbfName = tf
	}
	if inArray(ext, vectorExts) {
		thumbfName = fName
	}

	// Write to the DB.
	meta := models.JSON{}
	if isImage {
		meta = models.JSON{
			"width":  width,
			"height": height,
		}
	}
	m, err := app.core.InsertMedia(fName, thumbfName, contentType, meta, uploadProvider, app.media, authID)
	if err != nil {
		cleanUp = true
		return err
	}
	return c.JSON(http.StatusOK, okResp{m})
}

// handleGetMedia handles retrieval of uploaded media.
func handleGetMedia(c echo.Context) error {
	var (
		app   = c.Get("app").(*App)
		pg    = app.paginator.NewFromURL(c.Request().URL.Query())
		query = c.FormValue("query")
		id, _ = strconv.Atoi(c.Param("id"))
	)

	authID := c.Request().Header.Get("X-Auth-ID")

	if authID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "authid is required")
	}
	uploadProvider, err := app.core.GetUploadProvider(authID)
	if err != nil {
		return err
	}
	var uploadData map[string]interface{}
	if uploadProvider == "filesystem" {
		uploadData, err = app.core.GetFileSystemUploadData(authID)
		if err != nil {
			return err
		}
	} else {
		uploadData, err = app.core.GetS3UploadData(authID)
		if err != nil {
			return err
		}
	}
	initSettings("SELECT JSON_OBJECT_AGG(key, value) AS settings FROM settings WHERE authid = $1;", db, ko, authID)
	app.media = initMediaStore(uploadProvider, uploadData)
	if app.media == nil {
		log.Println("app.media is nil during initialization")
	}

	app.manager = initCampaignManager(app.queries, app.constants, app)
	app.messengers[emailMsgr] = initSMTPMessenger(app.manager)
	for _, m := range initPostbackMessengers(app.manager) {
		app.messengers[m.Name()] = m
	}
	for _, m := range app.messengers {
		app.manager.AddMessenger(m)
	}

	// Fetch one list.
	if id > 0 {
		out, err := app.core.GetMedia(id, "", app.media, authID)
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, okResp{out})
	}

	res, total, err := app.core.QueryMedia(uploadProvider, app.media, query, pg.Offset, pg.Limit, authID)
	if err != nil {
		return err
	}

	out := models.PageResults{
		Results: res,
		Total:   total,
		Page:    pg.Page,
		PerPage: pg.PerPage,
	}

	return c.JSON(http.StatusOK, okResp{out})
}

// deleteMedia handles deletion of uploaded media.
func handleDeleteMedia(c echo.Context) error {
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

	if err := app.core.DeleteMedia(id, authID); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, okResp{true})
}

// processImage reads the image file and returns thumbnail bytes and
// the original image's width, and height.
func processImage(file *multipart.FileHeader) (*bytes.Reader, int, int, error) {
	src, err := file.Open()
	if err != nil {
		return nil, 0, 0, err
	}
	defer src.Close()

	img, err := imaging.Decode(src)
	if err != nil {
		return nil, 0, 0, err
	}

	// Encode the image into a byte slice as PNG.
	var (
		thumb = imaging.Resize(img, thumbnailSize, 0, imaging.Lanczos)
		out   bytes.Buffer
	)
	if err := imaging.Encode(&out, thumb, imaging.PNG); err != nil {
		return nil, 0, 0, err
	}

	b := img.Bounds().Max
	return bytes.NewReader(out.Bytes()), b.X, b.Y, nil
}
