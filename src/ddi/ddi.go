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
	ddiStagingStageREST                  = "/stage"
	ddiStagingDeleteREST                 = "/delete"
	locationURLPropertyName              = "url"
	locationDDIAreaPropertyName          = "ddi_area"
	locationHPCStagingAreaPropertyName   = "hpc_staging_area"
	locationCloudStagingAreaPropertyName = "cloud_staging_area"
)

// Client is the client interface to Distrbuted Data Infrastructure (DDI) service
type Client interface {
	SubmitDDIToCloudDataTransfer(token, ddiSourcePath, cloudStagingAreaDestinationPath string) (string, error)
	SubmitDDIDataDeletion(token, path string) (string, error)
	SubmitCloudStagingAreaDataDeletion(token, path string) (string, error)
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
	hpcStagingArea := locationProps.GetString(locationHPCStagingAreaPropertyName)
	if hpcStagingArea == "" {
		return nil, errors.Errorf("No HPC staging area defined in location")
	}
	cloudStagingArea := locationProps.GetString(locationCloudStagingAreaPropertyName)
	if cloudStagingArea == "" {
		return nil, errors.Errorf("No Cloud staging area defined in location")
	}

	return &ddiClient{
		ddiArea:          ddiArea,
		hpcStagingArea:   hpcStagingArea,
		cloudStagingArea: cloudStagingArea,
		httpClient:       getHTTPClient(url),
	}, nil
}

type ddiClient struct {
	ddiArea          string
	hpcStagingArea   string
	cloudStagingArea string
	httpClient       *httpclient
}

// SubmitDDIToCloudDataTransfer submits a data transfer request from DDI to Cloud
func (d *ddiClient) SubmitDDIToCloudDataTransfer(token, ddiSourcePath, cloudStagingAreaDestinationPath string) (string, error) {

	request := DataTransferRequest{
		SourceSystem: d.ddiArea,
		SourcePath:   ddiSourcePath,
		TargetSystem: d.cloudStagingArea,
		TargetPath:   cloudStagingAreaDestinationPath,
	}
	var response SubmittedRequestInfo
	err := d.httpClient.doRequest(http.MethodPost, ddiStagingStageREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit DDI %s to Cloud %s data transfer", ddiSourcePath, cloudStagingAreaDestinationPath)
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

// SubmitCloudStagingAreaDataDeletion submits a Cloud staging area data deletion request
func (d *ddiClient) SubmitCloudStagingAreaDataDeletion(token, path string) (string, error) {

	request := DeleteDataRequest{
		TargetSystem: d.cloudStagingArea,
		TargetPath:   path,
	}
	var response SubmittedRequestInfo
	err := d.httpClient.doRequest(http.MethodDelete, ddiStagingDeleteREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit Cloud staging area data %s deletion", path)
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
