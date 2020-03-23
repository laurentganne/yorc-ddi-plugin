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
	"net/http"
	"path"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
)

const (
	ddiStagingStageREST             = "/stage"
	ddiStagingDeleteREST            = "/delete"
	locationURLPropertyName         = "url"
	locationDDIAreaPropertyName     = "ddi_area"
	locationStagingAreaPropertyName = "staging_area"
	locationDSSAreaPropertyName     = "dss_area"
)

// Client is the client interface to Distrbuted Data Infrastructure (DDI) service
type Client interface {
	SubmitDDIToCloudDataTransfer(token, ddiSourcePath, dssDestinationPath string) (string, error)
	SubmitDDIDataDeletion(token, path string) (string, error)
	SubmitDSSDataDeletion(token, path string) (string, error)
	GetDataTransferRequestStatus(token, requestID string) (string, error)
	GetDeletionRequestStatus(token, requestID string) (string, error)
}

// GetClient returns a DDI client for a given location
func GetClient(locationProps config.DynamicMap) (Client, error) {

	url := locationProps.GetString(locationURLPropertyName)
	if url == "" {
		return nil, errors.Errorf("No URL defined in DDI location configuration")
	}
	ddiArea := locationProps.GetString(locationDDIAreaPropertyName)
	if ddiArea == "" {
		return nil, errors.Errorf("No DDI area defined in location")
	}
	stagingArea := locationProps.GetString(locationStagingAreaPropertyName)
	if stagingArea == "" {
		return nil, errors.Errorf("No staging area defined in location")
	}
	dssArea := locationProps.GetString(locationDSSAreaPropertyName)
	if dssArea == "" {
		return nil, errors.Errorf("No DSS area defined in location")
	}

	return &ddiClient{
		ddiArea:     ddiArea,
		stagingArea: stagingArea,
		dssArea:     dssArea,
		httpClient:  getHTTPClient(url),
	}, nil
}

type ddiClient struct {
	ddiArea     string
	stagingArea string
	dssArea     string
	httpClient  *httpclient
}

// SubmitDDIToCloudDataTransfer submits a data transfer request from DDI to Cloud
func (d *ddiClient) SubmitDDIToCloudDataTransfer(token, ddiSourcePath, dssDestinationPath string) (string, error) {

	request := DataTransferRequest{
		SourceSystem: d.ddiArea,
		SourcePath:   ddiSourcePath,
		TargetSystem: d.dssArea,
		TargetPath:   dssDestinationPath,
	}
	var response SubmittedRequestInfo
	err := d.httpClient.doRequest(http.MethodPost, ddiStagingStageREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit DDI %s to Cloud %s data transfer", ddiSourcePath, dssDestinationPath)
	}

	return response.RequestID, err
}

// SubmitDDIDataDeletion submits a DDI data deletion request
func (d *ddiClient) SubmitDDIDataDeletion(token, path string) (string, error) {

	request := DeleteDataRequest{
		TargetSystem: d.ddiArea,
		TargetPath:   path,
	}
	var response SubmittedRequestInfo
	err := d.httpClient.doRequest(http.MethodPost, ddiStagingDeleteREST,
		[]int{http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit DDI data %s deletion", path)
	}

	return response.RequestID, err
}

// SubmitDSSDataDeletion submits a DSS data deletion request
func (d *ddiClient) SubmitDSSDataDeletion(token, path string) (string, error) {

	request := DeleteDataRequest{
		TargetSystem: d.dssArea,
		TargetPath:   path,
	}
	var response SubmittedRequestInfo
	err := d.httpClient.doRequest(http.MethodDelete, ddiStagingDeleteREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit DSS data %s deletion", path)
	}

	return response.RequestID, err
}

func (d *ddiClient) GetDataTransferRequestStatus(token, requestID string) (string, error) {

	var response RequestStatus
	err := d.httpClient.doRequest(http.MethodGet, path.Join(ddiStagingStageREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for data transfer request %s", requestID)
	}

	return response.Status, err
}

func (d *ddiClient) GetDeletionRequestStatus(token, requestID string) (string, error) {

	var response RequestStatus
	err := d.httpClient.doRequest(http.MethodGet, path.Join(ddiStagingDeleteREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for data transfer request %s", requestID)
	}

	return response.Status, err
}
