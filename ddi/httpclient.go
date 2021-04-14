// Copyright 2020 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddi

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/log"
)

type httpclient struct {
	*http.Client
	baseURL string
}

func getHTTPClient(URL string) *httpclient {

	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
	}

	return &httpclient{
		baseURL: URL,
		Client:  &http.Client{Transport: tr},
	}
}

// NewRequest returns a new HTTP request
func (c *httpclient) newRequest(method, path string, body io.Reader) (*http.Request, error) {
	return http.NewRequest(method, c.baseURL+path, body)
}

func (c *httpclient) doRequest(method, path string, expectedStatuses []int, token string, payload, result interface{}) error {

	var reqBody io.Reader
	if payload != nil {
		jsonParam, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		reqBody = bytes.NewBuffer(jsonParam)
	}

	log.Debugf("Sending request %s to %s", method, c.baseURL+path)

	request, err := c.newRequest(method, path, reqBody)
	if err != nil {
		return err
	}
	if token != "" {
		request.Header.Add("Authorization", fmt.Sprintf("Basic %s", token))
	}
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	response, err := c.Do(request)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	foundExpectedStatus := false
	for _, expectedStatus := range expectedStatuses {
		if response.StatusCode == expectedStatus {
			foundExpectedStatus = true
			break
		}
	}
	if !foundExpectedStatus {
		return errors.Errorf("Expected HTTP Status code in %v, got %d, reason %q",
			expectedStatuses, response.StatusCode, response.Status)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return errors.Wrap(err, "Failed to read response")
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return errors.Wrapf(err, "Failed to unmarshall response %s", string(body))
	}

	return err
}