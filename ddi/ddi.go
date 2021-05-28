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
	"encoding/json"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/log"
)

const (
	// TaskStatusPendingMsg is the message returned when a task is pending
	TaskStatusPendingMsg = "Task still in the queue, or task does not exist"
	// TaskStatusInProgressMsg is the message returned when a task is in progress
	TaskStatusInProgressMsg = "In progress"
	// TaskStatusTransferCompletedMsg is the message returned when a transfer is completed
	TaskStatusTransferCompletedMsg = "Transfer completed"
	// TaskStatusReplicationCompletedMsg is the message returned when a replication is completed
	TaskStatusReplicationCompletedMsg = "Replication completed"
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
	TaskStatusMsgSuffixAlreadyDisabled = "IP not found"
	// TaskStatusDoneMsg is the message returned when a task is done
	TaskStatusDoneMsg = "Done"

	ReplicationStatusParentDataset        = "Parent dataset. Dataset is replicated"
	ReplicationStatusReplicaDataset       = "Replica dataset. Dataset is replicated"
	ReplicationStatusDatasetNotReplicated = "Dataset is not replicated"
	ReplicationStatusNoSuchDataset        = "Dataset doesn't exist or you don't have permission to access it"

	invalidTokenError = "Invalid Token"

	enableCloudAccessREST           = "/cloud/add"
	disableCloudAccessREST          = "/cloud/remove"
	ddiStagingStageREST             = "/stage"
	ddiStagingDeleteREST            = "/delete"
	ddiStagingReplicateREST         = "/replicate"
	ddiStagingReplicationStatusREST = "/replication/status"
	ddiStagingDatasetInfoREST       = "/data/size"
	ddiDatasetSearchREST            = "/search/metadata"
	ddiDatasetListingREST           = "/listing"

	locationStagingURLPropertyName = "staging_url"
	locationSSHFSURLPropertyName   = "sshfs_url"
	locationDatasetURLPropertyName = "dataset_url"
	// LocationDDIAreaPropertyName is the property defining the DDI area name of a DDI location
	LocationDDIAreaPropertyName = "ddi_area"
	// LocationCloudStagingAreaNamePropertyName is the property defining the cloud staging area name of a DDI location
	LocationCloudStagingAreaNamePropertyName             = "cloud_staging_area_name"
	locationCloudStagingAreaRemoteFileSystemPropertyName = "cloud_staging_area_remote_file_system"
	locationCloudStagingAreaMountTypePropertyName        = "cloud_staging_area_mount_type"
	locationCloudStagingAreaMountOptionsPropertyName     = "cloud_staging_area_mount_options"
	locationCloudStagingAreaUserIDPropertyName           = "cloud_staging_area_user_id"
	locationCloudStagingAreaGroupIDPropertyName          = "cloud_staging_area_group_id"
)

// RefreshTokenFunc is a type of function provided by the caller to refresh a token when needed
type RefreshTokenFunc func() (newAccessToken string, err error)

// Client is the client interface to Distrbuted Data Infrastructure (DDI) service
type Client interface {
	CreateEmptyDatasetInProject(token, project string, metadata Metadata) (string, error)
	IsAlive() bool
	ListDataSet(token, datasetID, access, project string, recursive bool) (DatasetListing, error)
	GetDisableCloudAccessRequestStatus(token, requestID string) (string, error)
	GetEnableCloudAccessRequestStatus(token, requestID string) (string, error)
	SubmitCloudStagingAreaDataDeletion(token, path string) (string, error)
	SubmitCloudToDDIDataTransfer(metadata Metadata, token, cloudStagingAreaSourcePath, ddiDestinationPath, encryption, compression string) (string, error)
	SubmitDDIDatasetInfoRequest(token, targetSystem, ddiPath string) (string, error)
	SubmitDDIDataDeletion(token, path string) (string, error)
	SubmitDDIToCloudDataTransfer(metadata Metadata, token, ddiSourcePath, cloudStagingAreaDestinationPath, encryption, compression string) (string, error)
	SubmitDDIToHPCDataTransfer(metadata Metadata, token, ddiSourcePath, targetSystem, hpcDirectoryPath, encryption, compression, heappeURL string, jobID, taskID int64) (string, error)
	SubmitHPCToDDIDataTransfer(metadata Metadata, token, sourceSystem, hpcDirectoryPath, ddiPath, encryption, compression, heappeURL string, jobID, taskID int64) (string, error)
	SubmitDDIReplicationRequest(token, sourceSystem, sourcePath, targetSystem string) (string, error)
	GetCloudStagingAreaProperties() LocationCloudStagingArea
	GetDDIDatasetInfoRequestStatus(token, requestID string) (string, string, string, string, error)
	GetDataTransferRequestStatus(token, requestID string) (string, string, error)
	GetDeletionRequestStatus(token, requestID string) (string, error)
	GetCloudStagingAreaName() string
	GetDatasetURL() string
	GetDDIAreaName() string
	GetReplicationsRequestStatus(token, requestID string) (string, string, string, error)
	GetReplicationStatus(token, targetSystem, targetPath string) (string, error)
	GetSshfsURL() string
	GetStagingURL() string
	SearchDataset(token string, metadata Metadata) ([]DatasetSearchResult, error)
	SubmitEnableCloudAccess(token, ipAddress string) (string, error)
	SubmitDisableCloudAccess(token, ipAddress string) (string, error)
}

// GetClient returns a DDI client for a given location
func GetClient(locationProps config.DynamicMap, refreshTokenFunc RefreshTokenFunc) (Client, error) {

	url := locationProps.GetString(locationStagingURLPropertyName)
	if url == "" {
		return nil, errors.Errorf("No %s property defined in DDI location configuration", locationStagingURLPropertyName)
	}
	sshfsURL := locationProps.GetString(locationSSHFSURLPropertyName)
	if sshfsURL == "" {
		return nil, errors.Errorf("No %s property defined in DDI location configuration", locationSSHFSURLPropertyName)
	}
	datasetURL := locationProps.GetString(locationDatasetURLPropertyName)
	if datasetURL == "" {
		return nil, errors.Errorf("No %s property defined in DDI location configuration", locationDatasetURLPropertyName)
	}
	ddiArea := locationProps.GetString(LocationDDIAreaPropertyName)
	if ddiArea == "" {
		return nil, errors.Errorf("No %s property defined in DDI location configuration", LocationDDIAreaPropertyName)
	}
	var cloudStagingArea LocationCloudStagingArea
	cloudStagingArea.Name = locationProps.GetString(LocationCloudStagingAreaNamePropertyName)
	cloudStagingArea.RemoteFileSystem = locationProps.GetString(locationCloudStagingAreaRemoteFileSystemPropertyName)
	cloudStagingArea.MountType = locationProps.GetString(locationCloudStagingAreaMountTypePropertyName)
	cloudStagingArea.MountOptions = locationProps.GetString(locationCloudStagingAreaMountOptionsPropertyName)
	cloudStagingArea.UserID = locationProps.GetString(locationCloudStagingAreaUserIDPropertyName)
	cloudStagingArea.GroupID = locationProps.GetString(locationCloudStagingAreaGroupIDPropertyName)

	return &ddiClient{
		ddiArea:           ddiArea,
		cloudStagingArea:  cloudStagingArea,
		httpStagingClient: getHTTPClient(url, refreshTokenFunc),
		httpDatasetClient: getHTTPClient(datasetURL, refreshTokenFunc),
		StagingURL:        url,
		SshfsURL:          sshfsURL,
		DatasetURL:        datasetURL,
	}, nil
}

type ddiClient struct {
	ddiArea           string
	cloudStagingArea  LocationCloudStagingArea
	httpStagingClient *httpclient
	httpDatasetClient *httpclient
	StagingURL        string
	SshfsURL          string
	DatasetURL        string
}

// IsAlive checks if the staging URL is accessible
func (d *ddiClient) IsAlive() bool {

	response, err := http.Get(d.StagingURL)
	if err != nil {
		log.Printf("DDI client isAlive(): request to %s returned %s\n", d.StagingURL, err.Error())
		return false
	}
	response.Body.Close()
	log.Debugf("DDI client isAlive(): request to %s returned status %d\n", d.StagingURL, response.StatusCode)
	return response.StatusCode == 200 || response.StatusCode == 301 || response.StatusCode == 302
}

// SubmitEnableCloudAccess submits a request to enable the access to the Cloud
// staging area for a given IP address
func (d *ddiClient) SubmitEnableCloudAccess(token, ipAddress string) (string, error) {

	var response SubmittedRequestInfo
	err := d.httpStagingClient.doRequest(http.MethodPost, path.Join(enableCloudAccessREST, ipAddress),
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
	err := d.httpStagingClient.doRequest(http.MethodPost, path.Join(disableCloudAccessREST, ipAddress),
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
	err := d.httpStagingClient.doRequest(http.MethodGet, path.Join(enableCloudAccessREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for enable cloud access request %s", requestID)
	}

	return response.Status, err
}

// GetEnableCloudAccessRequestStatus returns the status of a request to disable a cloud access
func (d *ddiClient) GetDisableCloudAccessRequestStatus(token, requestID string) (string, error) {

	var response RequestStatus
	err := d.httpStagingClient.doRequest(http.MethodGet, path.Join(disableCloudAccessREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for enable cloud access request %s", requestID)
	}

	return response.Status, err
}

// SubmitDDIToCloudDataTransfer submits a data transfer request from DDI to Cloud
func (d *ddiClient) SubmitDDIToCloudDataTransfer(metadata Metadata, token, ddiSourcePath, cloudStagingAreaDestinationPath, encryption, compression string) (string, error) {

	request := DataTransferRequest{
		Metadata:     metadata,
		SourceSystem: d.ddiArea,
		SourcePath:   ddiSourcePath,
		TargetSystem: d.cloudStagingArea.Name,
		TargetPath:   cloudStagingAreaDestinationPath,
		Encryption:   encryption,
		Compression:  compression,
	}

	requestStr, _ := json.Marshal(request)
	log.Debugf("Submitting DDI staging request %s", string(requestStr))

	var response SubmittedRequestInfo
	err := d.httpStagingClient.doRequest(http.MethodPost, ddiStagingStageREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit DDI %s to Cloud %s data transfer", ddiSourcePath, cloudStagingAreaDestinationPath)
	}

	return response.RequestID, err
}

// SubmitDDIDatasetInfoRequest submits request to get the size and number of files of a dataset
func (d *ddiClient) SubmitDDIDatasetInfoRequest(token, targetSystem, ddiPath string) (string, error) {

	request := DatasetInfoRequest{
		TargetSystem: targetSystem,
		TargetPath:   ddiPath,
	}

	requestStr, _ := json.Marshal(request)
	log.Debugf("Submitting DDI staging request %s", string(requestStr))

	var response SubmittedRequestInfo
	err := d.httpStagingClient.doRequest(http.MethodPost, ddiStagingDatasetInfoREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit DDI %s dataset %s info request", targetSystem, ddiPath)
	}

	return response.RequestID, err
}

// SubmitCloudToDDIDataTransfer submits a data transfer request from Cloud to DDI
func (d *ddiClient) SubmitCloudToDDIDataTransfer(metadata Metadata, token, cloudStagingAreaSourcePath, ddiDestinationPath, encryption, compression string) (string, error) {

	request := DataTransferRequest{
		Metadata:     metadata,
		SourceSystem: d.cloudStagingArea.Name,
		SourcePath:   cloudStagingAreaSourcePath,
		TargetSystem: d.ddiArea,
		TargetPath:   ddiDestinationPath,
		Encryption:   encryption,
		Compression:  compression,
	}
	var response SubmittedRequestInfo
	err := d.httpStagingClient.doRequest(http.MethodPost, ddiStagingStageREST,
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
	err := d.httpStagingClient.doRequest(http.MethodPost, ddiStagingDeleteREST,
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
	err := d.httpStagingClient.doRequest(http.MethodDelete, ddiStagingDeleteREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit Cloud staging area data %s deletion", path)
	}

	return response.RequestID, err
}

// SubmitDDIToHPCDataTransfer submits a data transfer request from DDI to HPC
func (d *ddiClient) SubmitDDIToHPCDataTransfer(metadata Metadata, token, ddiSourcePath, targetSystem, hpcDirectoryPath, encryption, compression, heappeURL string, jobID, taskID int64) (string, error) {

	request := HPCDataTransferRequest{
		DataTransferRequest{
			Metadata:     metadata,
			SourceSystem: d.ddiArea,
			SourcePath:   ddiSourcePath,
			TargetSystem: targetSystem,
			TargetPath:   hpcDirectoryPath,
			Encryption:   encryption,
			Compression:  compression,
		},
		DataTransferRequestHPCExtension{
			HEAppEURL: heappeURL,
			JobID:     jobID,
			TaskID:    taskID,
		},
	}

	requestStr, _ := json.Marshal(request)
	log.Debugf("Submitting DDI data trasnfer request %s", string(requestStr))

	var response SubmittedRequestInfo
	err := d.httpStagingClient.doRequest(http.MethodPost, ddiStagingStageREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit DDI %s to HPC %s %s %s data transfer", ddiSourcePath, heappeURL, targetSystem, hpcDirectoryPath)
	}

	return response.RequestID, err
}

// SubmitHPCToDDIDataTransfer submits a data transfer request from HPC to DDI
func (d *ddiClient) SubmitHPCToDDIDataTransfer(metadata Metadata, token, sourceSystem, hpcDirectoryPath, ddiPath, encryption, compression, heappeURL string, jobID, taskID int64) (string, error) {

	request := HPCDataTransferRequest{
		DataTransferRequest{
			Metadata:     metadata,
			SourceSystem: sourceSystem,
			SourcePath:   hpcDirectoryPath,
			TargetSystem: d.ddiArea,
			TargetPath:   ddiPath,
			Encryption:   encryption,
			Compression:  compression,
		},
		DataTransferRequestHPCExtension{
			HEAppEURL: heappeURL,
			JobID:     jobID,
			TaskID:    taskID,
		},
	}

	requestStr, _ := json.Marshal(request)
	log.Debugf("Submitting DDI data trasbfer request %s", string(requestStr))

	var response SubmittedRequestInfo
	err := d.httpStagingClient.doRequest(http.MethodPost, ddiStagingStageREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit HPC %s %s %s to DDI %s data transfer", heappeURL, sourceSystem, hpcDirectoryPath, ddiPath)
	}

	return response.RequestID, err
}

// SubmitDDIReplication submits a replication request
func (d *ddiClient) SubmitDDIReplicationRequest(token, sourceSystem, sourcePath, targetSystem string) (string, error) {

	request := DataTransferRequest{
		SourceSystem: sourceSystem,
		SourcePath:   sourcePath,
		TargetSystem: targetSystem,
	}

	requestStr, _ := json.Marshal(request)
	log.Debugf("Submitting DDI replication request %s", string(requestStr))

	var response SubmittedRequestInfo
	err := d.httpStagingClient.doRequest(http.MethodPost, ddiStagingReplicateREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit replication of %s %s to %s", sourceSystem, sourcePath, targetSystem)
	}

	return response.RequestID, err
}

// GetDDIDatasetInfoRequestStatus returns the status of a dataset info request
func (d *ddiClient) GetDDIDatasetInfoRequestStatus(token, requestID string) (string, string, string, string, error) {

	var response DatasetInfo
	err := d.httpStagingClient.doRequest(http.MethodGet, path.Join(ddiStagingDatasetInfoREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for data transfer request %s", requestID)
	}

	return response.Result, response.Size, response.NumberOfFiles, response.NumberOfSmallFiles, err
}

// GetDataTransferRequestStatus returns the status of a data transfer request
// and the path of the transferred data on the destination
func (d *ddiClient) GetDataTransferRequestStatus(token, requestID string) (string, string, error) {

	var response RequestStatus
	var err error
	done := false
	// Retrying several times as this call regularly returns an error 500 Internal Server Error
	for i := 1; i < 5 && !done; i++ {
		err = d.httpStagingClient.doRequest(http.MethodGet, path.Join(ddiStagingStageREST, requestID),
			[]int{http.StatusOK}, token, nil, &response)
		done = (err == nil)
		if err != nil {
			err = errors.Wrapf(err, "Failed to submit get status for data transfer request %s", requestID)
			if strings.Contains(err.Error(), "502 Bad Gateway") {
				log.Printf("Attempt %d of get data transfer request status from %s for request %s failed on error 502 Bad Gateway\n",
					i, d.httpStagingClient.baseURL, requestID)
			} else {
				break
			}
			time.Sleep(5 * time.Second)
		}

	}

	return response.Status, response.TargetPath, err
}

// GetDeletionRequestStatus returns the status of a deletion request
func (d *ddiClient) GetDeletionRequestStatus(token, requestID string) (string, error) {

	var response RequestStatus
	err := d.httpStagingClient.doRequest(http.MethodGet, path.Join(ddiStagingDeleteREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for data transfer request %s", requestID)
	}

	return response.Status, err
}

// GetReplicationsRequestStatus returns the status of a replication request
// and the path of the replicated dataset on the destination
func (d *ddiClient) GetReplicationsRequestStatus(token, requestID string) (string, string, string, error) {

	var response RequestStatus
	err := d.httpStagingClient.doRequest(http.MethodGet, path.Join(ddiStagingReplicateREST, requestID),
		[]int{http.StatusOK}, token, nil, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit get status for replication request %s", requestID)
	}

	return response.Status, response.TargetPath, response.PID, err
}

// GetReplicationStatus returns the replication startus of a dataset on a given location
func (d *ddiClient) GetReplicationStatus(token, targetSystem, targetPath string) (string, error) {

	request := ReplicationStatusRequest{
		TargetSystem: targetSystem,
		TargetPath:   targetPath,
	}
	var response ReplicationStatusResponse
	var err error
	done := false
	// Retrying several times as this call regularly returns an error 500 Internal Server Error
	for i := 1; i < 5 && !done; i++ {
		err = d.httpStagingClient.doRequest(http.MethodPost, ddiStagingReplicationStatusREST,
			[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
		done = (err == nil)
		if err != nil {
			err = errors.Wrapf(err, "Failed to submit replication status request for system %s path %s", targetSystem, targetPath)
			if strings.Contains(err.Error(), "500 Internal Server Error") {
				log.Printf("Attempt %d of submit replication status request to %s%s for %q %q failed on error 500 Internal server error\n",
					i, d.httpStagingClient.baseURL, ddiStagingReplicationStatusREST, targetSystem, targetPath)
			} else {
				break
			}
			time.Sleep(5 * time.Second)
		}
	}

	return response.Status, err
}

// GetCloudStagingAreaProperties returns properties of a Cloud Staging Area
func (d *ddiClient) GetCloudStagingAreaProperties() LocationCloudStagingArea {

	return d.cloudStagingArea
}

// GetDDIAreaName returns the DDI area name
func (d *ddiClient) GetDDIAreaName() string {

	return d.ddiArea
}

// GetCloudStagingAreaName returns the DDI cloud staging area name
func (d *ddiClient) GetCloudStagingAreaName() string {

	return d.cloudStagingArea.Name
}

// GetStagingURL returns the DDI API URL
func (d *ddiClient) GetStagingURL() string {

	return d.StagingURL
}

// GetSshfsURL returns the DDI API SSHFS URL
func (d *ddiClient) GetSshfsURL() string {

	return d.SshfsURL
}

// GetDatasetURL returns the DDI API dataset URL
func (d *ddiClient) GetDatasetURL() string {

	return d.DatasetURL
}

// SearchDataset searches datasets matching the metadata properties in argument
func (d *ddiClient) SearchDataset(token string, metadata Metadata) ([]DatasetSearchResult, error) {

	var response []DatasetSearchResult

	err := d.httpDatasetClient.doRequest(http.MethodPost, ddiDatasetSearchREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, metadata, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to search dataset with metadata %v", metadata)
	}

	return response, err
}

func (d *ddiClient) CreateEmptyDatasetInProject(token, project string, metadata Metadata) (string, error) {
	var response DatasetCreateResponse

	request := DatasetCreateRequest{
		PushMethod: "empty",
		Access:     "project",
		Project:    project,
		Metadata:   metadata,
	}
	err := d.httpDatasetClient.doRequest(http.MethodPost, "",
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to search dataset with request %v", request)
	}

	return response.InternalID, err

}

// ListDataSet lists the content of a dataset
func (d *ddiClient) ListDataSet(token, datasetID, access, project string, recursive bool) (DatasetListing, error) {

	var response DatasetListing

	request := DatasetListingRequest{
		InternalID: datasetID,
		Access:     access,
		Project:    project,
		Recursive:  recursive,
	}
	err := d.httpDatasetClient.doRequest(http.MethodPost, ddiDatasetListingREST,
		[]int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, token, request, &response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to search dataset with request %v", request)
	}

	return response, err
}
