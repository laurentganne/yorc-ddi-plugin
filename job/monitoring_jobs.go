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

package job

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/laurentganne/yorc-ddi-plugin/common"
	"github.com/laurentganne/yorc-ddi-plugin/ddi"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/prov/operations"
)

const (
	// EnableCloudAccessAction is the action of enabling the access to cloud staging area
	EnableCloudAccessAction = "enable-cloud-access"
	// DisableCloudAccessAction is the action of enabling the access to cloud staging area
	DisableCloudAccessAction = "disable-cloud-access"
	// DataTransferAction is the action of transferring a dataset
	DataTransferAction = "transfer-request-monitoring"
	// CloudDataDeleteAction is the action of deleting a dataset from Cloud storage
	CloudDataDeleteAction = "cloud-data-delete-monitoring"
	// GetDDIDatasetInfoAction is the action of getting info on a dataset (size, number of files)
	GetDDIDatasetInfoAction = "get-ddi-dataset-info-monitoring"
	// WaitForDatasetAction is the action of waiting for a dataset to appear in DDI
	WaitForDatasetAction = "wait-for-dataset"
	// StoreRunningHPCJobFilesToDDIAction is the action of storing files created/updated
	// by a running HEAppE job
	StoreRunningHPCJobFilesToDDIAction = "store-running-hpc-job-files"
	// StoreRunningHPCJobFilesToDDIAction is the action of storing files created/updated
	// by a running HEAppE job and grouping them in datasets according to a pattern
	StoreRunningHPCJobFilesGroupByDatasetAction = "store-running-hpc-job-files-group-by-dataset"

	requestStatusPending        = "PENDING"
	requestStatusRunning        = "RUNNING"
	requestStatusCompleted      = "COMPLETED"
	requestStatusFailed         = "FAILED"
	actionDataNodeName          = "nodeName"
	actionDataRequestID         = "requestID"
	actionDataTaskID            = "taskID"
	actionDataDDIProjectName    = "ddiProjectName"
	actionDataMetadata          = "metadata"
	actionDataFilesPatterns     = "files_patterns"
	actionDataElapsedTime       = "elapsed_time"
	actionDataTaskName          = "task_name"
	actionDataOperation         = "operation"
	actionDataGroupFilesPattern = "group_files_pattern"
	actionDataReplicationSites  = "replication_sites"
	actionDataEncrypt           = "encrypt"
	actionDataCompress          = "compress"

	datasetElementDirectoryType = "directory"
	datasetElementFileType      = "file"

	projectPathPattern = "project/proj%x"
)

// ActionOperator holds function allowing to execute an action
type ActionOperator struct {
}

// ChangedFile holds properties of a file created/updated by a job
type ChangedFile struct {
	FileName         string
	LastModifiedDate string
	GroupIdentifier  string `json:"groupIdentifier,omitempty"`
}

// StoredFileInfo holds properties of a file stored in DDI
type StoredFileInfo struct {
	LastModifiedDate string
	RequestID        string
	Status           string
	ErrorMessage     string
	GroupIdentifier  string
	NumberOfAttempts int
}

// ToBeStoredFileInfo holds properties of a file to be stored in DDI
type ToBeStoredFileInfo struct {
	GroupIdentifier        string
	LastModifiedDate       string
	CandidateToStorageDate string
}

// ReplicationInfo holds the request ID and status of a replication
type ReplicationInfo struct {
	RequestID        string
	Status           string
	ErrorMessage     string
	NumberOfAttempts int
}

// DatasetReplicationInfo holds replication info of a dataset over several locations
type DatasetReplicationInfo struct {
	DatasetPath string
	Replication map[string]ReplicationInfo // replication info per location
}

type actionData struct {
	taskID   string
	nodeName string
}

type hpcJobMonitoringInfo struct {
	deploymentID     string
	nodeName         string
	metadata         ddi.Metadata
	projectName      string
	defaultPath      string
	groupID          string
	replicationSites []string
}

type hpcTransferContextInfo struct {
	token            string
	deploymentID     string
	nodeName         string
	jobID            int64
	taskID           int64
	jobDirPath       string
	heappeURL        string
	defaultPath      string
	metadata         ddi.Metadata
	sourceSystem     string
	replicationSites []string
}

// ExecAction allows to execute and action
func (o *ActionOperator) ExecAction(ctx context.Context, cfg config.Configuration, taskID, deploymentID string, action *prov.Action) (bool, error) {
	log.Debugf("Execute Action with ID:%q, taskID:%q, deploymentID:%q", action.ID, taskID, deploymentID)

	var deregister bool
	var err error
	if action.ActionType == DataTransferAction || action.ActionType == CloudDataDeleteAction ||
		action.ActionType == EnableCloudAccessAction || action.ActionType == DisableCloudAccessAction ||
		action.ActionType == GetDDIDatasetInfoAction {
		deregister, err = o.monitorJob(ctx, cfg, deploymentID, action)
	} else if action.ActionType == WaitForDatasetAction {
		deregister, err = o.monitorDataset(ctx, cfg, deploymentID, action)
	} else if action.ActionType == StoreRunningHPCJobFilesToDDIAction {
		deregister, err = o.monitorRunningHPCJob(ctx, cfg, deploymentID, action)
	} else if action.ActionType == StoreRunningHPCJobFilesGroupByDatasetAction {
		deregister, err = o.monitorRunningHPCJob(ctx, cfg, deploymentID, action)
	} else {
		deregister = true
		err = errors.Errorf("Unsupported actionType %q", action.ActionType)
	}
	return deregister, err
}

func (o *ActionOperator) monitorJob(ctx context.Context, cfg config.Configuration, deploymentID string, action *prov.Action) (bool, error) {
	var deregister bool

	actionData, err := o.getActionData(action)
	if err != nil {
		return true, err
	}
	requestID, ok := action.Data[actionDataRequestID]
	if !ok {
		return true, errors.Errorf("Missing mandatory information requestID for actionType:%q", action.ActionType)
	}

	ddiClient, err := getDDIClient(ctx, cfg, deploymentID, actionData.nodeName)
	if err != nil {
		return true, err
	}

	token, err := common.GetAccessToken(ctx, deploymentID, actionData.nodeName)
	if err != nil {
		return true, err
	}

	var status string
	var targetPath string
	var size string
	var numberOfFiles string
	var numberOfSmallFiles string
	switch action.ActionType {
	case EnableCloudAccessAction:
		status, err = ddiClient.GetEnableCloudAccessRequestStatus(token, requestID)
	case DisableCloudAccessAction:
		status, err = ddiClient.GetDisableCloudAccessRequestStatus(token, requestID)
	case DataTransferAction:
		status, targetPath, err = ddiClient.GetDataTransferRequestStatus(token, requestID)
	case CloudDataDeleteAction:
		status, err = ddiClient.GetDeletionRequestStatus(token, requestID)
	case GetDDIDatasetInfoAction:
		status, size, numberOfFiles, numberOfSmallFiles, err = ddiClient.GetDDIDatasetInfoRequestStatus(token, requestID)
	default:
		err = errors.Errorf("Unsupported action %s", action.ActionType)
	}
	if err != nil {
		return true, err
	}

	requestStatus, errorMessage, err := o.getRequestStatusFromDDIStatus(status)
	if err != nil {
		return true, err
	}

	previousRequestStatus, err := deployments.GetInstanceStateString(ctx, deploymentID, actionData.nodeName, "0")
	if err != nil {
		return true, errors.Wrapf(err, "failed to get instance state for request %s", requestID)
	}

	// See if monitoring must be continued and set job state if terminated
	switch requestStatus {
	case requestStatusCompleted:
		// Store the target path in case of a transfer request
		if targetPath != "" {
			// Check if this was a file transfer or a dataset transfer
			var fileName string
			val, err := deployments.GetInstanceAttributeValue(ctx, deploymentID, actionData.nodeName, "0", fileNameConsulAttribute)
			if err == nil && val != nil {
				fileName = val.RawString()
			}
			var destPath string
			if fileName == "" {
				destPath = targetPath
			} else {
				destPath = path.Join(targetPath, fileName)
			}
			err = deployments.SetAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
				destinationDatasetPathConsulAttribute, destPath)
			if err != nil {
				return false, errors.Wrapf(err, "Failed to store DDI dataset path attribute value %s", destPath)
			}

			err = deployments.SetCapabilityAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
				dataTransferCapability, destinationDatasetPathConsulAttribute, destPath)
			if err != nil {
				return false, errors.Wrapf(err, "Failed to store DDI dataset path capability attribute value %s", destPath)
			}
		} else if action.ActionType == GetDDIDatasetInfoAction {
			// Store dataset info
			e := common.DDIExecution{
				DeploymentID: deploymentID,
				NodeName:     actionData.nodeName,
			}
			err = e.SetDatasetInfoCapabilitySizeAttribute(ctx, size)
			if err != nil {
				return false, err
			}
			err = e.SetDatasetInfoCapabilityNumberOfFilesAttribute(ctx, numberOfFiles)
			if err != nil {
				return false, err
			}
			err = e.SetDatasetInfoCapabilityNumberOfSmallFilesAttribute(ctx, numberOfSmallFiles)
			if err != nil {
				return false, err
			}

		}

		// job has been done successfully : unregister monitoring
		deregister = true

	case requestStatusPending, requestStatusRunning:
		// job's still running or its state is about to be set definitively: monitoring is keeping on (deregister stays false)
	default:
		// Other cases as FAILED, CANCELED : error is return with job state and job info is logged
		deregister = true
		// Log event containing all the slurm information

		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(fmt.Sprintf("request %s status: %s, reason: %s", requestID, requestStatus, errorMessage))
		// Error to be returned
		err = errors.Errorf("Request ID %s finished unsuccessfully with status: %s, reason: %s", requestID, requestStatus, errorMessage)
	}

	// Print state change
	if previousRequestStatus != requestStatus {
		err := deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", requestStatus)
		if err != nil {
			log.Printf("Failed to set instance %s %s state %s: %s", deploymentID, actionData.nodeName, requestStatus, err.Error())
		}
	}

	return deregister, err
}

func (o *ActionOperator) getRequestStatusFromDDIStatus(ddiStatus string) (string, string, error) {
	var requestStatus string
	var errorMessage string
	var err error
	switch {
	case ddiStatus == ddi.TaskStatusPendingMsg:
		requestStatus = requestStatusPending
	case ddiStatus == ddi.TaskStatusInProgressMsg:
		requestStatus = requestStatusRunning
	case ddiStatus == ddi.TaskStatusTransferCompletedMsg:
		requestStatus = requestStatusCompleted
	case ddiStatus == ddi.TaskStatusDataDeletedMsg:
		requestStatus = requestStatusCompleted
	case ddiStatus == ddi.TaskStatusCloudAccessEnabledMsg:
		requestStatus = requestStatusCompleted
	case ddiStatus == ddi.TaskStatusDisabledMsg:
		requestStatus = requestStatusCompleted
	case ddiStatus == ddi.TaskStatusDoneMsg:
		requestStatus = requestStatusCompleted
	case strings.HasPrefix(ddiStatus, ddi.TaskStatusFailureMsgPrefix):
		if strings.HasSuffix(ddiStatus, ddi.TaskStatusMsgSuffixAlreadyEnabled) {
			requestStatus = requestStatusCompleted
		} else if strings.HasSuffix(ddiStatus, ddi.TaskStatusMsgSuffixAlreadyDisabled) {
			requestStatus = requestStatusCompleted
		} else {
			requestStatus = requestStatusFailed
			errorMessage = ddiStatus[(len(ddi.TaskStatusFailureMsgPrefix) - 1):]
		}
	default:
		err = errors.Errorf("Unexpected status :%q", ddiStatus)
	}

	return requestStatus, errorMessage, err

}

func (o *ActionOperator) monitorDataset(ctx context.Context, cfg config.Configuration, deploymentID string, action *prov.Action) (bool, error) {
	var (
		deregister bool
		ok         bool
	)

	actionData, err := o.getActionData(action)
	if err != nil {
		return true, err
	}
	// Add dataset metadata
	metadataStr, ok := action.Data[actionDataMetadata]
	if !ok {
		return true, errors.Errorf("Missing mandatory information metadata for actionType %s", action.ActionType)
	}
	var metadata ddi.Metadata
	err = json.Unmarshal([]byte(metadataStr), &metadata)
	if err != nil {
		return true, errors.Wrapf(err, "Wrong format for metadata %s for actionType %s", metadataStr, action.ActionType)
	}

	var filesPatterns []string
	filesPatternsStr := action.Data[actionDataFilesPatterns]
	if filesPatternsStr != "" {
		err = json.Unmarshal([]byte(filesPatternsStr), &filesPatterns)
		if err != nil {
			return true, errors.Wrapf(err, "Wrong format for files patterns %s for actionType %s", filesPatternsStr, action.ActionType)
		}

	}

	// TODO: check all DDI clients
	ddiClient, err := getDDIClient(ctx, cfg, deploymentID, actionData.nodeName)
	if err != nil {
		return true, err
	}

	if action.ActionType != WaitForDatasetAction {
		return true, errors.Errorf("Unsupported action %s", action.ActionType)
	}

	token, err := common.GetAccessToken(ctx, deploymentID, actionData.nodeName)
	if err != nil {
		return true, err
	}

	// First search if there is a dataset with the expected metadata
	results, err := ddiClient.SearchDataset(token, metadata)
	if err != nil {
		return true, errors.Wrapf(err, "failed search datasets with metadata %v", metadata)
	}

	requestStatus := requestStatusRunning
	type datasetResult struct {
		datasetID        string
		datasetPath      string
		matchingFilePath []string
	}
	var datasetResults []datasetResult

	for _, datasetRes := range results {
		var listing ddi.DatasetListing
		if len(filesPatterns) > 0 {
			listing, err = ddiClient.ListDataSet(token, datasetRes.Location.InternalID,
				datasetRes.Location.Access, datasetRes.Location.Project, true)
			if err != nil {
				return true, errors.Wrapf(err, "failed to get contents of dataset %s", datasetRes.Location.InternalID)
			}
		}
		projectPath := getDDIProjectPath(datasetRes.Location.Project)
		datasetPath := path.Join(projectPath, datasetRes.Location.InternalID)
		hasMatchingContent := true
		var matchingResults []string
		for _, fPattern := range filesPatterns {
			matchingPaths, err := o.findMatchingContent(&listing, fPattern, projectPath)
			if err != nil {
				return true, err
			}
			if len(matchingPaths) == 0 {
				hasMatchingContent = false
				break
			}

			matchingResults = append(matchingResults, matchingPaths...)

		}

		if hasMatchingContent {
			newResult := datasetResult{
				datasetID:        datasetRes.Location.InternalID,
				datasetPath:      datasetPath,
				matchingFilePath: matchingResults,
			}
			datasetResults = append(datasetResults, newResult)
		}

	}

	var result datasetResult
	if len(datasetResults) > 0 {
		requestStatus = requestStatusCompleted
		deregister = true
		result = datasetResults[len(datasetResults)-1]
	}

	previousRequestStatus, err := deployments.GetInstanceStateString(ctx, deploymentID, actionData.nodeName, "0")
	if err != nil {
		return true, errors.Wrapf(err, "failed to get instance state for deployment %s node %s", deploymentID, actionData.nodeName)
	}

	// See if monitoring must be continued and set job state if terminated
	if requestStatus == requestStatusCompleted {
		// Update node attributes
		err = deployments.SetAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
			datasetPathConsulAttribute, result.datasetPath)
		if err != nil {
			return false, errors.Wrapf(err, "Failed to store DDI dataset path attribute value %s", result.datasetPath)
		}

		err = deployments.SetCapabilityAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
			datasetFilesProviderCapability, datasetPathConsulAttribute, result.datasetPath)
		if err != nil {
			return false, errors.Wrapf(err, "Failed to store DDI dataset path capability attribute value %s", result.datasetPath)
		}

		err = deployments.SetAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
			datasetIDConsulAttribute, result.datasetID)
		if err != nil {
			return false, errors.Wrapf(err, "Failed to store DDI dataset ID attribute value %s", result.datasetID)
		}

		err = deployments.SetCapabilityAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
			datasetFilesProviderCapability, datasetIDConsulAttribute, result.datasetID)
		if err != nil {
			return false, errors.Wrapf(err, "Failed to store DDI dataset ID capability attribute value %s", result.datasetID)
		}

		err = deployments.SetAttributeComplexForAllInstances(ctx, deploymentID, actionData.nodeName,
			datasetFilesConsulAttribute, result.matchingFilePath)
		if err != nil {
			return false, errors.Wrapf(err, "Failed to store DDI matching file paths attribute value %v", result.matchingFilePath)
		}

		err = deployments.SetCapabilityAttributeComplexForAllInstances(ctx, deploymentID, actionData.nodeName,
			datasetFilesProviderCapability, datasetFilesConsulAttribute, result.matchingFilePath)
		if err != nil {
			return false, errors.Wrapf(err, "Failed to store DDI dataset ID capability attribute value %v", result.matchingFilePath)
		}

	}

	// Print state change
	if previousRequestStatus != requestStatus {
		err := deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", requestStatus)
		if err != nil {
			log.Printf("Failed to set instance %s %s state %s: %s", deploymentID, actionData.nodeName, requestStatus, err.Error())
		}
	}

	return deregister, err
}

func (o *ActionOperator) monitorRunningHPCJob(ctx context.Context, cfg config.Configuration, deploymentID string, action *prov.Action) (bool, error) {
	var (
		deregister bool
		ok         bool
	)

	if action.ActionType != StoreRunningHPCJobFilesToDDIAction &&
		action.ActionType != StoreRunningHPCJobFilesGroupByDatasetAction {
		return true, errors.Errorf("Unsupported action %s", action.ActionType)
	}

	actionData, err := o.getActionData(action)
	if err != nil {
		return true, err
	}

	elapsedTimeStr, ok := action.Data[actionDataElapsedTime]
	if !ok {
		return true, errors.Errorf("Missing mandatory information %s for actionType %s", actionDataElapsedTime, action.ActionType)
	}
	elapsedTime, err := strconv.Atoi(elapsedTimeStr)
	if err != nil {
		return true, errors.Wrapf(err, "Failed to parse int elapsed time for deployment %s node %s, value %s",
			deploymentID, actionData.nodeName, elapsedTimeStr)
	}

	elapsedDuration := time.Duration(elapsedTime) * time.Minute

	taskName := action.Data[actionDataTaskName]

	var filesPatternsProperty []string
	filesPatternsStr := action.Data[actionDataFilesPatterns]
	if filesPatternsStr != "" {
		err = json.Unmarshal([]byte(filesPatternsStr), &filesPatternsProperty)
		if err != nil {
			return true, errors.Wrapf(err, "Wrong format for files patterns %s for actionType %s", filesPatternsStr, action.ActionType)
		}

	}

	operationStr, ok := action.Data[actionDataOperation]
	if !ok {
		return true, errors.Errorf("Missing mandatory information %s for actionType %s", actionDataOperation, action.ActionType)
	}

	var opStore prov.Operation
	err = json.Unmarshal([]byte(operationStr), &opStore)
	if err != nil {
		return true, errors.Wrapf(err, "Failed to unmarshall operation %s", operationStr)
	}

	groupFilesPattern := action.Data[actionDataGroupFilesPattern]
	var replicationSites []string
	if action.Data[actionDataReplicationSites] != "" {
		err = json.Unmarshal([]byte(action.Data[actionDataReplicationSites]), &replicationSites)
		if err != nil {
			return true, errors.Wrapf(err, "Failed to parse action data %s property for operation %v : %s",
				actionDataReplicationSites, opStore, action.Data[actionDataReplicationSites])
		}
	}

	// Get encryption/compression settings
	encrypt, ok := action.Data[actionDataEncrypt]
	if !ok {
		encrypt = "no"
	}
	compress, ok := action.Data[actionDataCompress]
	if !ok {
		compress = "no"
	}

	// Get the list of already created dataset for each group and their replication status
	var datasetReplication map[string]DatasetReplicationInfo
	if groupFilesPattern != "" {
		val, err := deployments.GetInstanceAttributeValue(ctx, deploymentID, actionData.nodeName, "0", datasetReplicationConsulAttribute)
		if err == nil && val != nil && val.RawString() != "" {
			err = json.Unmarshal([]byte(val.RawString()), &datasetReplication)
			if err != nil {
				err = errors.Wrapf(err, "Failed to parse map of dataset paths %s", val.RawString())
				return true, err
			}
		}
	}

	ddiProjectName := action.Data[actionDataDDIProjectName]

	metadataStr, ok := action.Data[actionDataMetadata]
	if !ok {
		return true, errors.Errorf("Missing mandatory information metadata for actionType %s", action.ActionType)
	}
	var metadata ddi.Metadata
	err = json.Unmarshal([]byte(metadataStr), &metadata)
	if err != nil {
		return true, errors.Wrapf(err, "Wrong format for metadata %s for actionType %s", metadataStr, action.ActionType)
	}

	// Refresh input values
	envInputs, _, err := operations.ResolveInputsWithInstances(
		ctx, deploymentID, actionData.nodeName, actionData.taskID, opStore, nil, nil)
	if err != nil {
		return false, errors.Wrapf(err, "Failed to get env inputs for %s", operationStr)
	}

	jobState := strings.ToLower(o.getValueFromEnv(jobStateEnvVar, envInputs))
	var jobDone bool
	switch jobState {
	case "initial", "creating", "created", "submitting", "submitted", "pending":
		return deregister, err
	case "executed", "completed", "failed", "canceled":
		jobDone = true
	default:
		jobDone = false
	}
	log.Printf("LOLO job status %s\n", jobState)
	ddiClient, err := getDDIClient(ctx, cfg, deploymentID, actionData.nodeName)
	if err != nil {
		return true, err
	}

	// Get the dataset path
	var datasetPath string
	val, err := deployments.GetInstanceAttributeValue(ctx, deploymentID, actionData.nodeName, "0", destinationDatasetPathConsulAttribute)
	if err == nil && val != nil && val.RawString() != "" {
		datasetPath = val.RawString()
	}

	// Get details on files already stored
	var storedFiles map[string]StoredFileInfo
	val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, actionData.nodeName, "0", storedFilesConsulAttribute)
	if err == nil && val != nil && val.RawString() != "" {
		err = json.Unmarshal([]byte(val.RawString()), &storedFiles)
		if err != nil {
			err = errors.Wrapf(err, "Failed to parse map of stored files %s", val.RawString())
			return true, err
		}
	}

	// Get details of files to be stored
	var toBeStoredFiles map[string]ToBeStoredFileInfo
	val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, actionData.nodeName, "0", toBeStoredFilesConsulAttribute)
	if err != nil {
		log.Printf("LOLO deployments.GetInstanceAttributeValue %s %s returns %+v\n", deploymentID, actionData, err)
	}
	if err == nil && val != nil && val.RawString() != "" {
		err = json.Unmarshal([]byte(val.RawString()), &toBeStoredFiles)
		if err != nil {
			err = errors.Wrapf(err, "Failed to parse map of to be stored files %s", val.RawString())
			return true, err
		}
	} else {
		log.Printf("LOLO deployments.GetInstanceAttributeValue %s %s returns no value\n", deploymentID, actionData)
	}

	log.Printf("LOLO consul for %s has toBeStoredFiles %+v\n", actionData.nodeName, toBeStoredFiles)

	// The task ID has to be added as prefix to file patterns if a task name was specified
	strVal := o.getValueFromEnv(tasksNameIdEnvVar, envInputs)
	if strVal == "" {
		return true, errors.Errorf("Failed to get map of tasks name-id from associated job")
	}
	var tasksNameID map[string]string
	err = json.Unmarshal([]byte(strVal), &tasksNameID)
	if err != nil {
		return true, errors.Wrapf(err, "Failed to unmarshall map od task name - task id %s", strVal)
	}

	var taskIDStr string
	var filesPatterns []string
	if taskName != "" {
		taskIDStr = tasksNameID[taskName]
		for _, fPattern := range filesPatternsProperty {
			newPattern := fmt.Sprintf("%s///%s", taskIDStr, fPattern)
			filesPatterns = append(filesPatterns, newPattern)
		}
		if len(filesPatterns) == 0 {
			// Define at list a pattern to match the task files
			newPattern := fmt.Sprintf("%s///.*", taskIDStr)
			filesPatterns = append(filesPatterns, newPattern)
		}
	} else {
		// just need to define a task ID for the REST request
		for _, v := range tasksNameID {
			taskIDStr = v
			break
		}
	}
	var taskID int64
	if taskIDStr == "" {
		return true, errors.Errorf("Failed to find a task ID for task %s in associated job", taskName)
	} else {
		taskID, err = strconv.ParseInt(taskIDStr, 10, 64)
		if err != nil {
			err = errors.Wrapf(err, "Unexpected Task ID ID value %q for deployment %s node %s",
				taskIDStr, deploymentID, actionData.nodeName)
			return true, err
		}
	}

	startDateStr := o.getValueFromEnv(jobStartDateEnvVar, envInputs)
	if startDateStr == "" {
		log.Debugf("Nothing to store yet for %s %s, related HEAppE job not yet started", deploymentID, actionData.nodeName)
		return deregister, err
	}

	layout := "2006-01-02T15:04:05"
	startTime, err := time.Parse(layout, startDateStr)
	if err != nil {
		err = errors.Wrapf(err, "Failed to parse job start time %s, expected layout like %s", startDateStr, layout)
		return true, err
	}
	// The job has started
	// Getting the list of files and keeping only those created/updated after the start date
	changedFilesStr := o.getValueFromEnv(jobChangedFilesEnvVar, envInputs)

	if changedFilesStr == "" {
		log.Debugf("Nothing to store yet for %s %s, related HEAppE job has not yet created/updated files", deploymentID, actionData.nodeName)
		return deregister, err

	}
	var changedFiles []ChangedFile
	err = json.Unmarshal([]byte(changedFilesStr), &changedFiles)
	if err != nil {
		return true, errors.Wrapf(err, "Wrong format for changed files %s for actionType %s", changedFilesStr, action.ActionType)
	}

	// Keeping only the files since job start not already stored, removing any input file added before
	// and removing files not matching the filters if any is defined
	var newFilesUpdates []ChangedFile
	layout = "2006-01-02T15:04:00Z"
	for _, changedFile := range changedFiles {
		changedTime, err := time.Parse(layout, changedFile.LastModifiedDate)
		if err != nil {
			log.Debugf("Deployment %s node %s ignoring last modified date %s which has not the expected layout %s",
				deploymentID, actionData.nodeName, changedFile.LastModifiedDate, layout)
			continue
		}

		if startTime.Before(changedTime) {
			storedFile, ok := storedFiles[changedFile.FileName]
			if ok {
				if storedFile.LastModifiedDate == changedFile.LastModifiedDate {
					// Already stored
					continue
				} else {
					// Updated since last store
					delete(storedFiles, changedFile.FileName)
				}
			}
			matches, err := common.MatchesFilter(changedFile.FileName, filesPatterns)
			if err != nil {
				return true, errors.Wrapf(err, "Failed to check if file %s matches filters %v", changedFile.FileName, filesPatterns)
			}
			if matches {
				changedFile.GroupIdentifier = o.getGroupIdentifier(changedFile.FileName, groupFilesPattern)
				newFilesUpdates = append(newFilesUpdates, changedFile)
			} else {
				log.Debugf("ignoring file %s not matching patterns %+v\n", changedFile.FileName, filesPatterns)
			}
		}
	}

	log.Debugf("new files updates: %+v\n", newFilesUpdates)
	log.Printf("LOLO new files updates: %+v\n", newFilesUpdates)

	// Update the maps of files to be stored
	toBeStoredUpdated := make(map[string]ToBeStoredFileInfo)
	toStore := make(map[string]ChangedFile)
	currentTime := time.Now()
	currentDate := currentTime.Format(time.RFC3339)
	for _, changedFile := range newFilesUpdates {
		if jobDone {
			toStore[changedFile.FileName] = changedFile
			continue
		}
		// Job is not yet done, checking the last modification date and elapsed time
		// to see if files to be stored can be stored
		toBeStoredFile, ok := toBeStoredFiles[changedFile.FileName]
		if ok {
			if toBeStoredFile.LastModifiedDate == changedFile.LastModifiedDate {
				// Already known to be stored
				// Checking if the time to wait for its storage has elapse
				insertTime, _ := time.Parse(time.RFC3339, toBeStoredFile.CandidateToStorageDate)
				duration := currentTime.Sub(insertTime)
				log.Printf("LOLO duration %s expecting %s\ncurrent time %s insert time %s\n",
					duration.String(), elapsedDuration.String(), currentTime.String(), insertTime.String())
				if duration >= elapsedDuration {
					log.Printf("New file to store: %s\n", changedFile.FileName)
					toStore[changedFile.FileName] = changedFile
				} else {
					toBeStoredUpdated[changedFile.FileName] = toBeStoredFile
				}
			}
		} else {
			log.Printf("LOLO new file changed: %s date %s\n", changedFile.FileName, changedFile.LastModifiedDate)
			toBeStoredUpdated[changedFile.FileName] = ToBeStoredFileInfo{
				GroupIdentifier:        changedFile.GroupIdentifier,
				LastModifiedDate:       changedFile.LastModifiedDate,
				CandidateToStorageDate: currentDate,
			}
		}
	}

	log.Debugf("Files to be stored updated: %+v\n", toBeStoredUpdated)
	log.Printf("LOLO Files to be stored updated: %+v\n", toBeStoredUpdated)

	// Save the new to be stored values
	err = deployments.SetAttributeComplexForAllInstances(ctx, deploymentID, actionData.nodeName,
		toBeStoredFilesConsulAttribute, toBeStoredUpdated)
	if err != nil {
		return deregister, errors.Wrapf(err, "Failed to store %s %s %s value %+v",
			deploymentID, actionData.nodeName, toBeStoredFilesConsulAttribute, toBeStoredUpdated)
	}

	// Submit requests to store files
	jobDirPath := o.getValueFromEnv(hpcDirectoryPathEnvVar, envInputs)
	if jobDirPath == "" {
		return true, errors.Errorf("Failed to get HPC directory path")
	}

	serverFQDN := o.getValueFromEnv(hpcServerEnvVar, envInputs)
	if serverFQDN == "" {
		return true, errors.Errorf("Failed to get HPC server")
	}
	res := strings.SplitN(serverFQDN, ".", 2)
	sourceSystem := res[0] + "_home"
	heappeJobIDStr := o.getValueFromEnv(heappeJobIDEnvVar, envInputs)
	if heappeJobIDStr == "" {
		return true, errors.Errorf("Failed to get ID of associated job")
	}
	heappeJobID, err := strconv.ParseInt(heappeJobIDStr, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "Unexpected Job ID value %q for deployment %s node %s",
			heappeJobIDStr, deploymentID, actionData.nodeName)
		return true, err
	}

	heappeURL := o.getValueFromEnv(heappeURLEnvVar, envInputs)
	if heappeURL == "" {
		return true, errors.Errorf("Failed to get HEAppE URL of job %d", heappeJobID)
	}

	token, err := common.GetAccessToken(ctx, deploymentID, actionData.nodeName)
	if err != nil {
		return true, err
	}

	for name, fileDetails := range toStore {
		hpcJobMonitoringInfo := hpcJobMonitoringInfo{
			deploymentID:     deploymentID,
			nodeName:         actionData.nodeName,
			metadata:         metadata,
			projectName:      ddiProjectName,
			defaultPath:      datasetPath,
			groupID:          fileDetails.GroupIdentifier,
			replicationSites: replicationSites,
		}

		destPath, err := o.setDestinationDatasetPath(ctx, ddiClient, hpcJobMonitoringInfo, datasetReplication, token)
		if err != nil {
			return true, err
		}
		sourcePath := path.Join(jobDirPath, name)
		requestID, err := ddiClient.SubmitHPCToDDIDataTransfer(metadata, token, sourceSystem,
			sourcePath, destPath, encrypt, compress, heappeURL, heappeJobID, taskID)
		if err != nil {
			return true, errors.Wrapf(err, "Failed to submit data transfer of %s to DDI", sourcePath)
		}

		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(
			fmt.Sprintf("Submitted request to store %s, request_id %s", sourcePath, requestID))

		storedFiles[name] = StoredFileInfo{
			LastModifiedDate: fileDetails.LastModifiedDate,
			RequestID:        requestID,
			Status:           requestStatusPending,
			GroupIdentifier:  fileDetails.GroupIdentifier,
		}
	}

	// Get an update of data transfer requests status
	hpcTransferInfo := hpcTransferContextInfo{
		token:            token,
		deploymentID:     deploymentID,
		nodeName:         actionData.nodeName,
		jobID:            heappeJobID,
		taskID:           taskID,
		defaultPath:      datasetPath,
		jobDirPath:       jobDirPath,
		heappeURL:        heappeURL,
		metadata:         metadata,
		sourceSystem:     sourceSystem,
		replicationSites: replicationSites,
	}
	remainingRequests, completedGroupsIDs, err := o.updateRequestsStatus(ctx, ddiClient,
		storedFiles, toBeStoredUpdated, datasetReplication, hpcTransferInfo, encrypt, compress)
	if err != nil {
		return true, err
	}

	log.Debugf("Files stored updated: %+v\n", storedFiles)
	log.Printf("LOLO Files stored updated: %+v\n", storedFiles)

	// Replicate completed datasets
	replicating, replicationDone, err := o.updateReplicationStatus(
		ctx, ddiClient, completedGroupsIDs, datasetReplication, hpcTransferInfo)
	if err != nil {
		return true, err
	}

	if len(replicationDone) > 0 {
		err = deployments.SetAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
			destinationDatasetPathConsulAttribute, datasetReplication[replicationDone[len(replicationDone)-1]].DatasetPath)
		if err != nil {
			return true, errors.Wrapf(err, "Failed to store %s %s %s value %s", deploymentID, actionData.nodeName, destinationDatasetPathConsulAttribute, "")
		}
	}

	if jobDone && (len(remainingRequests) > 0 || len(replicating) > 0) {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(
			fmt.Sprintf("Job %s done, but still %d transfer requests and %d replications to end",
				actionData.nodeName, len(remainingRequests), len(replicating)))
	}

	deregister = jobDone && len(remainingRequests) == 0 && len(replicating) == 0
	return deregister, err
}

func (o *ActionOperator) updateReplicationStatus(ctx context.Context, ddiClient ddi.Client,
	completedGroupsIDs []string, datasetReplication map[string]DatasetReplicationInfo,
	hpcTransferInfo hpcTransferContextInfo) ([]string, []string, error) {

	var replicatingIDs []string
	var replicationDone []string

	if len(hpcTransferInfo.replicationSites) == 0 {
		// Nothing to replicate
		return replicatingIDs, replicationDone, nil
	}

	// Update status of replications in progress and find which replication to (re)submit
	var replicationsToSubmit []string
	for groupID, datasetReplicationInfo := range datasetReplication {
		replications := datasetReplicationInfo.Replication

		for location, replicationInfo := range replications {
			if replicationInfo.RequestID == "" {
				replicationsToSubmit = append(replicationsToSubmit, groupID)
				continue
			}
			var requestStatus, errorMessage string
			ddiStatus, _, err := ddiClient.GetDataTransferRequestStatus(hpcTransferInfo.token, replicationInfo.RequestID)
			if err != nil {
				log.Printf("Failed to get status of DDI replication request %s : %s", replicationInfo.RequestID, err.Error())
				requestStatus = requestStatusFailed
				errorMessage = err.Error()
			} else {
				requestStatus, errorMessage, err = o.getRequestStatusFromDDIStatus(ddiStatus)
				if err != nil {
					log.Printf("Failed to match status of DDI replication request %s : %s", replicationInfo.RequestID, err.Error())
					requestStatus = requestStatusFailed
					errorMessage = err.Error()

				}
			}
			replicationInfo.ErrorMessage = errorMessage
			replicationInfo.Status = requestStatus
			if errorMessage != "" {
				replicationInfo.NumberOfAttempts = replicationInfo.NumberOfAttempts + 1
				replicationsToSubmit = append(replicationsToSubmit, groupID)
			} else if requestStatus != requestStatusCompleted {
				replicatingIDs = append(replicatingIDs, groupID)
			} else {
				replicationDone = append(replicationDone, groupID)
			}
			replications[location] = replicationInfo
		}
		datasetReplicationInfo.Replication = replications
		datasetReplication[groupID] = datasetReplicationInfo
	}

	// Submit new requests
	var err error
	for _, groupID := range replicationsToSubmit {
		replicatingIDs = append(replicatingIDs, groupID)
		replications := datasetReplication[groupID].Replication
		for location, replicationInfo := range replications {
			if replicationInfo.NumberOfAttempts > 5 {
				events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, hpcTransferInfo.deploymentID).RegisterAsString(
					fmt.Sprintf("Submitted request to replicate %s to %s, request_id %s failed at attempt %d with error %s",
						datasetReplication[groupID].DatasetPath, location, replicationInfo.RequestID, replicationInfo.NumberOfAttempts, replicationInfo.ErrorMessage))
				err = errors.Errorf("Failed to replicate %s to %s, DDI request %s error %s", datasetReplication[groupID].DatasetPath, location,
					replicationInfo.RequestID, replicationInfo.ErrorMessage)
				break
			}

			replicationInfo.RequestID, err = ddiClient.SubmitDDIReplicationRequest(hpcTransferInfo.token, ddiClient.GetDDIAreaName(),
				datasetReplication[groupID].DatasetPath, location)
			if err != nil {
				events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, hpcTransferInfo.deploymentID).RegisterAsString(
					fmt.Sprintf("Failed to submit replication of %s to %s, error %s",
						datasetReplication[groupID].DatasetPath, location, err.Error()))
				replicationInfo.RequestID = ""
				replicationInfo.ErrorMessage = err.Error()
				replicationInfo.Status = requestStatusFailed
				replicationInfo.NumberOfAttempts = replicationInfo.NumberOfAttempts + 1
			} else {
				replicationInfo.ErrorMessage = ""
				replicationInfo.Status = requestStatusPending
				replicationInfo.NumberOfAttempts = 0
			}

		}
	}

	// Save the new stored values
	saveErr := deployments.SetAttributeComplexForAllInstances(ctx, hpcTransferInfo.deploymentID, hpcTransferInfo.nodeName,
		datasetReplicationConsulAttribute, datasetReplication)
	if saveErr != nil {
		saveErr = errors.Wrapf(err, "Failed to store %s %s %s value %+v", hpcTransferInfo.deploymentID,
			hpcTransferInfo.nodeName, datasetReplicationConsulAttribute, datasetReplication)
		err = saveErr
	}

	return replicatingIDs, replicationDone, err
}

func (o *ActionOperator) updateRequestsStatus(ctx context.Context, ddiClient ddi.Client,
	storedFiles map[string]StoredFileInfo, toBeStored map[string]ToBeStoredFileInfo,
	datasetReplication map[string]DatasetReplicationInfo,
	hpcTransferInfo hpcTransferContextInfo, encrypt, compress string) (map[string]StoredFileInfo, []string, error) {

	remainingRequests := make(map[string]StoredFileInfo)
	failedRequests := make(map[string]StoredFileInfo)
	groupIDCompleteStatus := make(map[string]bool)
	var groupIDDone []string

	// A group ID is not yet complete if files are still to be stored for this group ID
	for _, tobeStoredInfo := range toBeStored {
		if tobeStoredInfo.GroupIdentifier != "" {
			groupIDCompleteStatus[tobeStoredInfo.GroupIdentifier] = false
		}
	}

	// Update the request status for all data transfer requests in progress
	for name, storedFileInfo := range storedFiles {
		status := storedFileInfo.Status
		if status == requestStatusCompleted {
			continue
		}

		if storedFileInfo.RequestID == "" {
			// Previous submission failed
			failedRequests[name] = storedFileInfo
			continue
		}

		var requestStatus, errorMessage string
		ddiStatus, _, err := ddiClient.GetDataTransferRequestStatus(hpcTransferInfo.token, storedFileInfo.RequestID)
		if err != nil {
			log.Printf("Failed to get status of DDI request %s : %s", storedFileInfo.RequestID, err.Error())
			requestStatus = requestStatusFailed
			errorMessage = err.Error()
		} else {
			requestStatus, errorMessage, err = o.getRequestStatusFromDDIStatus(ddiStatus)
			if err != nil {
				log.Printf("Failed to match status of DDI request %s : %s", storedFileInfo.RequestID, err.Error())
				requestStatus = requestStatusFailed
				errorMessage = err.Error()

			}
		}
		storedFileInfo.Status = requestStatus
		if errorMessage != "" {
			storedFileInfo.ErrorMessage = errorMessage
			failedRequests[name] = storedFileInfo
			if storedFileInfo.GroupIdentifier != "" {
				groupIDCompleteStatus[storedFileInfo.GroupIdentifier] = false
			}
		} else if requestStatus != requestStatusCompleted {
			remainingRequests[name] = storedFileInfo
			if storedFileInfo.GroupIdentifier != "" {
				groupIDCompleteStatus[storedFileInfo.GroupIdentifier] = false
			}
		} else if storedFileInfo.GroupIdentifier != "" {
			// Request completed
			_, found := groupIDCompleteStatus[storedFileInfo.GroupIdentifier]
			if !found {
				groupIDCompleteStatus[storedFileInfo.GroupIdentifier] = true
			}
		}
		storedFiles[name] = storedFileInfo
	}

	// Check group IDs for which there is no request pending anymore
	for groupID, done := range groupIDCompleteStatus {
		if done {
			groupIDDone = append(groupIDDone, groupID)
		}
	}

	// Retry requests that failed
	var err error
	for name, failedRequest := range failedRequests {
		failedRequest.NumberOfAttempts = failedRequest.NumberOfAttempts + 1
		if failedRequest.NumberOfAttempts > 5 {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, hpcTransferInfo.deploymentID).RegisterAsString(
				fmt.Sprintf("Submitted request to store %s, request_id %s failed at attempt %d with error %s",
					name, failedRequest.RequestID, failedRequest.NumberOfAttempts, failedRequest.ErrorMessage))
			err = errors.Errorf("Failed to store %s in DDI, DDI request %s error %s", name,
				failedRequest.RequestID, failedRequest.ErrorMessage)
			break
		}
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, hpcTransferInfo.deploymentID).RegisterAsString(
			fmt.Sprintf("Retrying submitted DDI request to store %s, request %s which failed at attempt %d with error %s",
				name, failedRequest.RequestID, failedRequest.NumberOfAttempts, failedRequest.ErrorMessage))
		sourcePath := path.Join(hpcTransferInfo.jobDirPath, name)
		var datasetPath string
		if failedRequest.GroupIdentifier == "" {
			datasetPath = hpcTransferInfo.defaultPath
		} else {
			datasetPath = datasetReplication[failedRequest.GroupIdentifier].DatasetPath
		}
		requestID, submitErr := ddiClient.SubmitHPCToDDIDataTransfer(hpcTransferInfo.metadata, hpcTransferInfo.token, hpcTransferInfo.sourceSystem,
			sourcePath, datasetPath, encrypt, compress,
			hpcTransferInfo.heappeURL, hpcTransferInfo.jobID, hpcTransferInfo.taskID)
		if submitErr != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, hpcTransferInfo.deploymentID).RegisterAsString(
				fmt.Sprintf("Failed to submit data transfer of %s to DDI, error %s",
					name, submitErr.Error()))
			failedRequest.RequestID = ""
			failedRequest.ErrorMessage = submitErr.Error()
			failedRequest.Status = requestStatusFailed
			failedRequest.NumberOfAttempts = failedRequest.NumberOfAttempts + 1
		} else {
			failedRequest.RequestID = requestID
			failedRequest.Status = requestStatusPending
			failedRequest.ErrorMessage = ""
			failedRequest.NumberOfAttempts = 0
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, hpcTransferInfo.deploymentID).RegisterAsString(
				fmt.Sprintf("Submitted request to store %s, request_id %s", sourcePath, requestID))
		}
		storedFiles[name] = failedRequest
		remainingRequests[name] = failedRequest

	}

	// Save the new stored values
	saveErr := deployments.SetAttributeComplexForAllInstances(ctx, hpcTransferInfo.deploymentID, hpcTransferInfo.nodeName,
		storedFilesConsulAttribute, storedFiles)
	if saveErr != nil {
		saveErr = errors.Wrapf(err, "Failed to store %s %s %s value %+v",
			hpcTransferInfo.deploymentID, hpcTransferInfo.nodeName, storedFilesConsulAttribute, storedFiles)
		err = saveErr
	}

	return remainingRequests, groupIDDone, err
}

func (o *ActionOperator) getGroupIdentifier(fileName, groupPattern string) string {
	var result string
	if groupPattern == "" {
		return result
	}

	// TODO : remove
	// re2 := regexp.MustCompile(`.*_t([0-9]+).dat`)
	// fmt.Printf("%q\n", re2.FindStringSubmatch("FA/udft_b0033_h0068_t00002.dat"))

	re := regexp.MustCompile(groupPattern)
	matches := re.FindStringSubmatch(fileName)
	if len(matches) > 1 {
		result = matches[1]
	}
	return result
}

func (o *ActionOperator) setDestinationDatasetPath(ctx context.Context, ddiClient ddi.Client,
	hpcJobMonitoringInfo hpcJobMonitoringInfo, datasetReplication map[string]DatasetReplicationInfo, token string) (string, error) {

	resultPath := hpcJobMonitoringInfo.defaultPath

	if hpcJobMonitoringInfo.groupID == "" {
		return resultPath, nil
	}

	existingDataset, ok := datasetReplication[hpcJobMonitoringInfo.groupID]
	if ok {
		return existingDataset.DatasetPath, nil
	}

	// Create a dataset
	metadata := hpcJobMonitoringInfo.metadata
	metadata.Title = fmt.Sprintf("%s - ID %s", metadata.Title, hpcJobMonitoringInfo.groupID)
	internalID, err := ddiClient.CreateEmptyDatasetInProject(token, hpcJobMonitoringInfo.projectName, metadata)
	if err != nil {
		return resultPath, errors.Wrapf(err, "Failed to create result dataset at %s for project %s metadata %v",
			ddiClient.GetDatasetURL(), hpcJobMonitoringInfo.projectName, metadata)
	}

	// Add replication info to dataset replication for new completed groups
	resultPath = path.Join(getDDIProjectPath(hpcJobMonitoringInfo.projectName), internalID)
	var replications map[string]ReplicationInfo
	if len(hpcJobMonitoringInfo.replicationSites) > 0 {
		replications = make(map[string]ReplicationInfo)
		for _, replicationSite := range hpcJobMonitoringInfo.replicationSites {
			replicationLocation := replicationSite + "_iRODS"
			replications[replicationLocation] = ReplicationInfo{}
		}
	}

	datasetReplication[hpcJobMonitoringInfo.groupID] = DatasetReplicationInfo{
		DatasetPath: resultPath,
		Replication: replications,
	}

	err = deployments.SetAttributeComplexForAllInstances(ctx, hpcJobMonitoringInfo.deploymentID, hpcJobMonitoringInfo.nodeName,
		datasetReplicationConsulAttribute, datasetReplication)
	if err != nil {
		return resultPath, errors.Wrapf(err, "Failed to store %s %s %s value %+v", hpcJobMonitoringInfo.deploymentID,
			hpcJobMonitoringInfo.nodeName, datasetReplicationConsulAttribute, datasetReplication)
	}

	return resultPath, err
}

func (o *ActionOperator) getValueFromEnv(envVarName string, envVars []*operations.EnvInput) string {

	var result string
	for _, envInput := range envVars {
		if envInput.Name == envVarName {
			result = envInput.Value
			break
		}
	}
	return result

}

func (o *ActionOperator) getActionData(action *prov.Action) (*actionData, error) {
	var ok bool
	actionData := &actionData{}
	// Check nodeName
	actionData.nodeName, ok = action.Data[actionDataNodeName]
	if !ok {
		return actionData, errors.Errorf("Missing mandatory information nodeName for actionType:%q", action.ActionType)
	}
	// Check taskID
	actionData.taskID, ok = action.Data[actionDataTaskID]
	if !ok {
		return actionData, errors.Errorf("Missing mandatory information taskID for actionType:%q", action.ActionType)
	}
	return actionData, nil
}

func (o *ActionOperator) findMatchingContent(listing *ddi.DatasetListing, fPattern, prefix string) ([]string, error) {

	var matchingPaths []string
	if listing.Type == datasetElementDirectoryType {
		newPrefix := path.Join(prefix, listing.Name)
		for _, content := range listing.Contents {
			contentMatchingPaths, err := o.findMatchingContent(content, fPattern, newPrefix)
			if err != nil {
				return matchingPaths, err
			}
			if len(contentMatchingPaths) > 0 {
				matchingPaths = append(matchingPaths, contentMatchingPaths...)
			}
		}
	} else if listing.Type == datasetElementFileType {
		matched, err := regexp.MatchString(fPattern, listing.Name)
		if err != nil {
			return matchingPaths, err
		}
		if matched {
			matchingPaths = append(matchingPaths, path.Join(prefix, listing.Name))
		}
	} else {
		return matchingPaths, errors.Errorf("Unexpected content type %s for content name %s", listing.Type, listing.Name)
	}

	return matchingPaths, nil
}

func getDDIProjectPath(projectName string) string {
	projectPath := fmt.Sprintf(projectPathPattern, md5.Sum([]byte(projectName)))
	return projectPath
}
