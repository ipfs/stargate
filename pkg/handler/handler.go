/*
Package handler implements the HTTP mechanics of sending StarGate responses

Note: much of this code is cribbed from https://github.com/filecoin-project/boost/blob/main/cmd/booster-http/server.go
*/
package handler

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/fatih/color"
	"github.com/ipfs/go-cid"
	stargate "github.com/ipfs/stargate/pkg"
	"github.com/ipfs/stargate/pkg/carwriter.go"
)

// Handler is a an HTTP Handler for a given StarGate AppResolver
type Handler struct {
	prefix      string
	appResolver stargate.AppResolver
}

// NewHandler constructs an http Handler for given prefix + appResolver
func NewHandler(prefix string, appResolver stargate.AppResolver) *Handler {
	return &Handler{
		prefix:      prefix,
		appResolver: appResolver,
	}
}

var _ http.Handler = (*Handler)(nil)

// writeErrorWatcher calls onError if there is an error writing to the writer
type writeErrorWatcher struct {
	http.ResponseWriter
	count   uint64
	onError func(err error)
}

func (w *writeErrorWatcher) Write(bz []byte) (int, error) {
	count, err := w.ResponseWriter.Write(bz)
	if err != nil {
		w.onError(err)
	}
	w.count += uint64(count)
	return count, err
}

const timeFmt = "2006-01-02T15:04:05.000Z0700"

func alog(l string, args ...interface{}) {
	alogAt(time.Now(), l, args...)
}

func alogAt(at time.Time, l string, args ...interface{}) {
	fmt.Printf(at.Format(timeFmt)+"\t"+l+"\n", args...)
}

func serveContent(w http.ResponseWriter, r *http.Request, content io.ReadSeeker) {
	// Set the Content-Type header explicitly so that http.ServeContent doesn't
	// try to do it implicitly
	w.Header().Set("Content-Type", "application/vnd.ipld.car+stargate")

	var writer http.ResponseWriter

	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	var err error
	writeErrWatcher := &writeErrorWatcher{ResponseWriter: w, onError: func(e error) {
		err = e
	}}

	writer = writeErrWatcher //Need writeErrWatcher to be of type writeErrorWatcher for addCommas()

	// Note that the last modified time is a constant value because the data
	// in a piece identified by a cid will never change.
	start := time.Now()
	alogAt(start, "%s\tGET %s", color.New(color.FgGreen).Sprintf("%d", http.StatusOK), r.URL)
	isGzipped := strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
	if isGzipped {
		// If Accept-Encoding header contains gzip then send a gzipped response

		gzwriter := gziphandler.GzipResponseWriter{
			ResponseWriter: writeErrWatcher,
		}
		// Close the writer to flush buffer
		defer gzwriter.Close()
		writer = &gzwriter
	}

	if r.Method == "HEAD" {
		// For an HTTP HEAD request ServeContent doesn't send any data (just headers)
		http.ServeContent(writer, r, "", time.Time{}, content)
		alog("%s\tHEAD %s", color.New(color.FgGreen).Sprintf("%d", http.StatusOK), r.URL)
		return
	}

	// Send the content
	http.ServeContent(writer, r, "", lastModified, content)

	// Write a line to the log
	end := time.Now()
	completeMsg := fmt.Sprintf("GET %s\n%s - %s: %s / %s bytes transferred",
		r.URL, end.Format(timeFmt), start.Format(timeFmt), time.Since(start), addCommas(writeErrWatcher.count))
	if isGzipped {
		completeMsg += " (gzipped)"
	}
	if err == nil {
		alogAt(end, "%s\t%s", color.New(color.FgGreen).Sprint("DONE"), completeMsg)
	} else {
		alogAt(end, "%s\t%s\n%s",
			color.New(color.FgRed).Sprint("FAIL"), completeMsg, err)
	}
}

func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	w.WriteHeader(status)
	w.Write([]byte("Error: " + msg)) //nolint:errcheck
	alog("%s\tGET %s\n%s",
		color.New(color.FgRed).Sprintf("%d", status), r.URL, msg)
}

// For data served by the endpoints in the HTTP server that never changes
// (eg pieces identified by a piece CID) send a cache header with a constant,
// non-zero last modified time.
var lastModified = time.UnixMilli(1)

func addCommas(count uint64) string {
	str := fmt.Sprintf("%d", count)
	for i := len(str) - 3; i > 0; i -= 3 {
		str = str[:i] + "," + str[i:]
	}
	return str
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// remove paths that are too short
	if len(r.URL.Path) <= len(h.prefix)+1 {
		msg := fmt.Sprintf("path '%s' is missing CID", r.URL.Path)
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}

	// remove prefix
	prefix, remaining := r.URL.Path[:len(h.prefix)+2], r.URL.Path[len(h.prefix)+2:]
	if prefix != "/"+h.prefix+"/" {
		msg := fmt.Sprintf("incorrect prefix -- expected: %s, got: %s", "/"+h.prefix+"/", prefix)
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}
	// parse root CID
	segments := strings.Split(remaining, "/")
	cidString, pathSegments := segments[0], segments[1:]
	rootCid, err := cid.Parse(cidString)
	if err != nil {
		msg := fmt.Sprintf("parsing  CID '%s': %s", cidString, err.Error())
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}
	// create a temporary file for the response (we want to serialize the whole thing to know
	// if it will be a success)
	responseFile, err := os.CreateTemp("", cidString+"-")
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "error setting up response")
		return
	}

	defer func() {
		_ = responseFile.Close()
		os.Remove(responseFile.Name())
	}()

	// write the response
	err = carwriter.WriteCar(r.Context(), responseFile, rootCid, pathSegments, stargate.Query(r.URL.Query()), h.appResolver)

	if err != nil {
		// check for not found errors while writing response
		var errNotFound stargate.ErrNotFound
		if errors.As(err, &errNotFound) {
			writeError(w, r, http.StatusNotFound, err.Error())
			return
		}
		var errPathError stargate.ErrPathError
		if errors.As(err, &errPathError) {
			writeError(w, r, http.StatusNotFound, err.Error())
			return
		}
		// otherwise 500
		writeError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	// serve the completed response with an OK status
	serveContent(w, r, responseFile)
}
