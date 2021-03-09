package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/reactivex/rxgo/v2"
	y3 "github.com/yomorun/y3-codec-golang"
	"github.com/yomorun/yomo/pkg/rx"
)

// NoiseDataKey represents the Tag of a Y3 encoded data packet
const NoiseDataKey = 0x10

var (
	MacrometaUrl    = ""
	Macrometaapikey = ""
)

func init() {
	MacrometaUrl = os.Getenv("macrometaUrl")
	if MacrometaUrl != "" {
		MacrometaUrl = fmt.Sprintf("https://api-%s/_fabric/_system/_api/document/yomo", strings.Split(MacrometaUrl, "https://")[1])
	}
	Macrometaapikey = fmt.Sprintf("apikey %s", os.Getenv("macrometaapikey"))
}

type NoiseData struct {
	Noise float32 `y3:"0x11"`
	Time  int64   `y3:"0x12"`
	From  string  `y3:"0x13"`
}

// save data to macrometa
var saveDocs = func(_ context.Context, i interface{}) (interface{}, error) {
	buf, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", MacrometaUrl, bytes.NewBuffer(buf))
	req.Header.Set("authorization", Macrometaapikey)
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		return nil, err

	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err

	}
	if !(resp.StatusCode == 201 || resp.StatusCode == 202) {
		return nil, errors.New("request fail")
	}

	defer resp.Body.Close()

	return fmt.Sprintf("⚡️ %d successfully stored in the macrometa", len(i.([]interface{}))), nil
}

// y3 callback
var callback = func(v []byte) (interface{}, error) {
	var mold NoiseData
	err := y3.ToObject(v, &mold)

	if err != nil {
		return nil, err
	}
	return mold, nil
}

// Handler will handle data in Rx way
func Handler(rxstream rx.RxStream) rx.RxStream {
	if MacrometaUrl == "" || Macrometaapikey == "" {
		panic("not found macrometaUrl or macrometaapikey.")
	}

	stream := rxstream.
		Subscribe(NoiseDataKey).
		OnObserve(callback).
		BufferWithTimeOrCount(rxgo.WithDuration(5*time.Second), 30).
		Map(saveDocs).
		StdOut().
		Encode(NoiseDataKey)
	return stream
}
