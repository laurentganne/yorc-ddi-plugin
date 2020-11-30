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
	// TaskStatusPendingMsg is the message returned when a task is pending
	TaskStatusPendingMsg = "Task still in the queue, or task does not exist"
	// TaskStatusInProgressMsg is the message returned when a task is in progress
	TaskStatusInProgressMsg = "In progress"
	// TaskStatusTransferCompletedMsg is the message returned when a ytansfer is completed
	TaskStatusTransferCompletedMsg = "Transfer completed"
	// TaskStatusDataDeletedMsg is the message returned when data is deleted
	TaskStatusDataDeletedMsg = "Data deleted"
	// TaskStatusCloudAccessEnabledMsg is the message returned when the access to cloud staging area is enabled
	TaskStatusCloudAccessEnabledMsg = "cloud nfs export added"
	// TaskStatusDisabledMsg is the message returned when the access to cloud staging area is enabled
	TaskStatusDisabledMsg = "cloud nfs export deleted"
	// TaskStatusFailureMsgPrefix is the the prefix used in task failure messages
	TaskStatusFailureMsgPrefix = "Task Failed, reason: "
	// TaskStatusMsgSuffixAlreadyEnabled is the the prefix used in task failing because the cloud access is already enabled
	TaskStatusMsgSuffixAlreadyEnabled = "IP export is already active"
	// TaskStatusMsgSuffixAlreadyDisabled is the the prefix used in task failing because the cloud access is already disabled
	TaskStatusMsgSuffixAlreadyDisabled                   = "IP not found"
	enableCloudAccessREST                                = "/cloud/add"
	disableCloudAccessREST                               = "/cloud/remove"
	ddiStagingStageREST                                  = "/stage"
	ddiStagingDeleteREST                                 = "/delete"
	locationStagingURLPropertyName                       = "staging_url"
	locationSSHFSURLPropertyName                         = "sshfs_url"
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
	SubmitEnableCloudAccess(token, ipAddress string) (string, error)
	SubmitDisableCloudAccess(token, ipAddress string) (string, error)
	GetEnableCloudAccessRequestStatus(token, requestID string) (string, error)
	GetDisableCloudAccessRequestStatus(token, requestID string) (string, error)
	SubmitDDIToCloudDataTransfer(metadata Metadata, token, ddiSourcePath, cloudStagingAreaDestinationPath string) (string, error)
	SubmitCloudToDDIDataTransfer(metadata Metadata, token, cloudStagingAreaSourcePath, ddiDestinationPath string) (string, error)
	SubmitDDIDataDeletion(token, path string) (string, error)
	SubmitDDIToHPCDataTransfer(metadata Metadata, token, ddiSourcePath, hpcDirectoryPath string) (string, error)
	SubmitCloudStagingAreaDataDeletion(token, path string) (string, error)
	GetDataTransferRequestStatus(token, requestID string) (string, string, error)
	GetDeletionRequestStatus(token, requestID string) (string, error)
	GetCloudStagingAreaProperties() LocationCloudStagingArea
	GetStagingURL() string
	GetSshfsURL() string
}

// GetClient returns a DDI client for a given location
func GetClient(locationProps config.DynamicMap) (Client, error) {

	url := locationProps.GetString(locationStagingURLPropertyName)
	if url == "" {
		return nil, errors.Errorf("No %s property defined in DDI location configuration", locationStagingURLPropertyName)
	}
	sshfsURL := locationProps.GetString(locationSSHFSURLPropertyName)
	if sshfsURL == "" {
		return nil, errors.Errorf("No %s property defined in DDI location configuration", locationSSHFSURLPropertyName)
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
		StagingURL:       url,
		SshfsURL:         sshfsURL,
	}, nil
}

type ddiClient struct {
	ddiArea          string
	hpcStagingArea   LocationHPCStagingArea
	cloudStagingArea LocationCloudStagingArea
	httpClient       *httpclient
	StagingURL       string
	SshfsURL         string
}

// SubmitEnableCloudAccess submits a request to enable the access to the Cloud
// staging area for a given IP address
func (d *ddiClient) SubmitEnableCloudAccess(token, ipAddress string) (string, error) {

	var response SubmittedRequestInfo
	err := d.httpClient.doRequest(http.MethodPost, path.Join(enableCloudAccessREST, ipAddress),
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit requrest do enable cloud staging area access to %s",
			ipAddress)
	}

	return response.RequestID, err
}

// SubmitDisableCloudAccess submits a request to disable the access to the Cloud
// staging area for a given IP address
func (d *ddiClient) SubmitDisableCloudAccess(token, ipAddress string) (string, error) {

	var response SubmittedRequestInfo
	err := d.httpClient.doRequest(http.MethodPost, path.Join(disableCloudAccessREST, ipAddress),
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit requrest do disable cloud staging area access to %s",
			ipAddress)
	}

	return response.RequestID, err
}

// GetEnableCloudAccessRequestStatus returns the status of a request to enable a cloud access
func (d *ddiClient) GetEnableCloudAccessRequestStatus(token, requestID string) (string, error) {

	var response RequestStatus
	err := d.httpClient.doRequest(http.MethodGet, path.Join(enableCloudAccessREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for enable cloud access request %s", requestID)
	}

	return response.Status, err
}

// GetEnableCloudAccessRequestStatus returns the status of a request to disable a cloud access
func (d *ddiClient) GetDisableCloudAccessRequestStatus(token, requestID string) (string, error) {

	var response RequestStatus
	err := d.httpClient.doRequest(http.MethodGet, path.Join(disableCloudAccessREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for enable cloud access request %s", requestID)
	}

	return response.Status, err
}

// SubmitDDIToCloudDataTransfer submits a data transfer request from DDI to Cloud
func (d *ddiClient) SubmitDDIToCloudDataTransfer(metadata Metadata, token, ddiSourcePath, cloudStagingAreaDestinationPath string) (string, error) {

	request := DataTransferRequest{
		Metadata:     metadata,
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
func (d *ddiClient) SubmitCloudToDDIDataTransfer(metadata Metadata, token, cloudStagingAreaSourcePath, ddiDestinationPath string) (string, error) {

	request := DataTransferRequest{
		Metadata:     metadata,
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

// SubmitDDIToHPCDataTransfer submits a data transfer request from DDI to HPC
func (d *ddiClient) SubmitDDIToHPCDataTransfer(metadata Metadata, token, ddiSourcePath, hpcDirectoryPath string) (string, error) {

	// TODO: implement DDI to HOC
	return "DDIToHPCTempRequestID", nil
}

// GetDataTransferRequestStatus returns the status of a data transfer request
// and the path of the transferred data on the destination
func (d *ddiClient) GetDataTransferRequestStatus(token, requestID string) (string, string, error) {

	var response RequestStatus
	err := d.httpClient.doRequest(http.MethodGet, path.Join(ddiStagingStageREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for data transfer request %s", requestID)
	}

	return response.Status, response.TargetPath, err
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

// GetURL returns the DDI API URL
func (d *ddiClient) GetStagingURL() string {

	return d.StagingURL
}

// GetSshfsURL returns the DDI API SSHFS URL
func (d *ddiClient) GetSshfsURL() string {

	return d.SshfsURL
}
