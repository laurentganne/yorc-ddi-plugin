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
	ddiStagingStageREST                                  = "/stage"
	ddiStagingDeleteREST                                 = "/delete"
	locationURLPropertyName                              = "url"
	locationDDIAreaPropertyName                          = "ddi_area"
	locationHPCStagingAreaNamePropertyName               = "hpc_staging_area_name"
	locationCloudStagingAreaNamePropertyName             = "cloud_staging_area_name"
	locationCloudStagingAreaRemoteFileSystemPropertyName = "cloud_staging_area_remote_file_system"
	locationCloudStagingAreaMountTypePropertyName        = "cloud_staging_area_mount_type"
	locationCloudStagingAreaMountOptionsPropertyName     = "cloud_staging_area_mount_options"
	locationCloudStagingAreaUserIDPropertyName           = "cloud_staging_area_user_id"
	locationCloudStagingAreaGroupIDPropertyName          = "cloud_staging_area_group_id"
)

// Client is the client interface to Distrbuted Data Infrastructure (DDI) service
type Client interface {
	SubmitDDIToCloudDataTransfer(token, ddiSourcePath, cloudStagingAreaDestinationPath string) (string, error)
	SubmitCloudToDDIDataTransfer(token, cloudStagingAreaSourcePath, ddiDestinationPath string) (string, error)
	SubmitDDIDataDeletion(token, path string) (string, error)
	SubmitCloudStagingAreaDataDeletion(token, path string) (string, error)
	GetDataTransferRequestStatus(token, requestID string) (string, error)
	GetDeletionRequestStatus(token, requestID string) (string, error)
	GetCloudStagingAreaProperties() LocationCloudStagingArea
}

// GetClient returns a DDI client for a given location
func GetClient(locationProps config.DynamicMap) (Client, error) {

	url := locationProps.GetString(locationURLPropertyName)
	if url == "" {
		return nil, errors.Errorf("No %s property defined in DDI location configuration", locationURLPropertyName)
	}
	ddiArea := locationProps.GetString(locationDDIAreaPropertyName)
	if ddiArea == "" {
		return nil, errors.Errorf("No %s property defined in DDI location configuration", locationDDIAreaPropertyName)
	}
	var hpcStagingArea LocationHPCStagingArea
	hpcStagingArea.Name = locationProps.GetString(locationHPCStagingAreaNamePropertyName)
	var cloudStagingArea LocationCloudStagingArea
	cloudStagingArea.Name = locationProps.GetString(locationCloudStagingAreaNamePropertyName)
	cloudStagingArea.RemoteFileSystem = locationProps.GetString(locationCloudStagingAreaRemoteFileSystemPropertyName)
	cloudStagingArea.MountType = locationProps.GetString(locationCloudStagingAreaMountTypePropertyName)
	cloudStagingArea.MountOptions = locationProps.GetString(locationCloudStagingAreaMountOptionsPropertyName)
	cloudStagingArea.UserID = locationProps.GetString(locationCloudStagingAreaUserIDPropertyName)
	cloudStagingArea.GroupID = locationProps.GetString(locationCloudStagingAreaGroupIDPropertyName)

	return &ddiClient{
		ddiArea:          ddiArea,
		hpcStagingArea:   hpcStagingArea,
		cloudStagingArea: cloudStagingArea,
		httpClient:       getHTTPClient(url),
	}, nil
}

type ddiClient struct {
	ddiArea          string
	hpcStagingArea   LocationHPCStagingArea
	cloudStagingArea LocationCloudStagingArea
	httpClient       *httpclient
}

// SubmitDDIToCloudDataTransfer submits a data transfer request from DDI to Cloud
func (d *ddiClient) SubmitDDIToCloudDataTransfer(token, ddiSourcePath, cloudStagingAreaDestinationPath string) (string, error) {

	request := DataTransferRequest{
		SourceSystem: d.ddiArea,
		SourcePath:   ddiSourcePath,
		TargetSystem: d.cloudStagingArea.Name,
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

// SubmitCloudToDDIDataTransfer submits a data transfer request from Cloud to DDI
func (d *ddiClient) SubmitCloudToDDIDataTransfer(token, cloudStagingAreaSourcePath, ddiDestinationPath string) (string, error) {

	request := DataTransferRequest{
		SourceSystem: d.cloudStagingArea.Name,
		SourcePath:   cloudStagingAreaSourcePath,
		TargetSystem: d.ddiArea,
		TargetPath:   ddiDestinationPath,
	}
	var response SubmittedRequestInfo
	err := d.httpClient.doRequest(http.MethodPost, ddiStagingStageREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit Cloud %s to DDI %s data transfer", cloudStagingAreaSourcePath, ddiDestinationPath)
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
		TargetSystem: d.cloudStagingArea.Name,
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

// GetDataTransferRequestStatus returns the status of a data transfer request
func (d *ddiClient) GetDataTransferRequestStatus(token, requestID string) (string, error) {

	var response RequestStatus
	err := d.httpClient.doRequest(http.MethodGet, path.Join(ddiStagingStageREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for data transfer request %s", requestID)
	}

	return response.Status, err
}

// GetDeletionRequestStatus returns the status of a deletion request
func (d *ddiClient) GetDeletionRequestStatus(token, requestID string) (string, error) {

	var response RequestStatus
	err := d.httpClient.doRequest(http.MethodGet, path.Join(ddiStagingDeleteREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for data transfer request %s", requestID)
	}

	return response.Status, err
}

// GetCloudStagingAreaProperties returns properties of a Cloud Staging Area
func (d *ddiClient) GetCloudStagingAreaProperties() LocationCloudStagingArea {

	return d.cloudStagingArea
}
