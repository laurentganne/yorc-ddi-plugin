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

	"github.com/laurentganne/yorc-ddi-plugin/v1/common"
	"github.com/laurentganne/yorc-ddi-plugin/v1/ddi"

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

	requestStatusPending        = "PENDING"
	requestStatusRunning        = "RUNNING"
	requestStatusCompleted      = "COMPLETED"
	requestStatusFailed         = "FAILED"
	actionDataNodeName          = "nodeName"
	actionDataRequestID         = "requestID"
	actionDataToken             = "token"
	actionDataTaskID            = "taskID"
	actionDataMetadata          = "metadata"
	actionDataFilesPatterns     = "files_patterns"
	actionDataElapsedTime       = "elapsed_time"
	actionDataTaskName          = "task_name"
	actionDataOperation         = "operation"
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
}

// StoredFileInfo holds properties of a file stored in DDI
type StoredFileInfo struct {
	LastModifiedDate string
	RequestID        string
}

// ToBeStoredFileInfo holds properties of a file to be stored in DDI
type ToBeStoredFileInfo struct {
	LastModifiedDate       string
	CandidateToStorageDate string
}

type actionData struct {
	token    string
	taskID   string
	nodeName string
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

	var status string
	var targetPath string
	var size string
	var numberOfFiles string
	var numberOfSmallFiles string
	switch action.ActionType {
	case EnableCloudAccessAction:
		status, err = ddiClient.GetEnableCloudAccessRequestStatus(actionData.token, requestID)
	case DisableCloudAccessAction:
		status, err = ddiClient.GetDisableCloudAccessRequestStatus(actionData.token, requestID)
	case DataTransferAction:
		status, targetPath, err = ddiClient.GetDataTransferRequestStatus(actionData.token, requestID)
	case CloudDataDeleteAction:
		status, err = ddiClient.GetDeletionRequestStatus(actionData.token, requestID)
	case GetDDIDatasetInfoAction:
		status, size, numberOfFiles, numberOfSmallFiles, err = ddiClient.GetDDIDatasetInfoRequestStatus(actionData.token, requestID)
	default:
		err = errors.Errorf("Unsupported action %s", action.ActionType)
	}
	if err != nil {
		return true, err
	}

	var requestStatus string
	var errorMessage string
	switch {
	case status == ddi.TaskStatusPendingMsg:
		requestStatus = requestStatusPending
	case status == ddi.TaskStatusInProgressMsg:
		requestStatus = requestStatusRunning
	case status == ddi.TaskStatusTransferCompletedMsg:
		requestStatus = requestStatusCompleted
	case status == ddi.TaskStatusDataDeletedMsg:
		requestStatus = requestStatusCompleted
	case status == ddi.TaskStatusCloudAccessEnabledMsg:
		requestStatus = requestStatusCompleted
	case status == ddi.TaskStatusDisabledMsg:
		requestStatus = requestStatusCompleted
	case status == ddi.TaskStatusDoneMsg:
		requestStatus = requestStatusCompleted
	case strings.HasPrefix(status, ddi.TaskStatusFailureMsgPrefix):
		if strings.HasSuffix(status, ddi.TaskStatusMsgSuffixAlreadyEnabled) {
			requestStatus = requestStatusCompleted
		} else if strings.HasSuffix(status, ddi.TaskStatusMsgSuffixAlreadyDisabled) {
			requestStatus = requestStatusCompleted
		} else {
			requestStatus = requestStatusFailed
			errorMessage = status[(len(ddi.TaskStatusFailureMsgPrefix) - 1):]
		}
	default:
		return true, errors.Errorf("Unexpected status :%q", status)
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
		return true, errors.Wrapf(err, "Wrong format for metadata %s for actioType %s", metadataStr, action.ActionType)
	}

	var filesPatterns []string
	filesPatternsStr := action.Data[actionDataFilesPatterns]
	if filesPatternsStr != "" {
		err = json.Unmarshal([]byte(filesPatternsStr), &filesPatterns)
		if err != nil {
			return true, errors.Wrapf(err, "Wrong format for files patterns %s for actioType %s", filesPatternsStr, action.ActionType)
		}

	}

	ddiClient, err := getDDIClient(ctx, cfg, deploymentID, actionData.nodeName)
	if err != nil {
		return true, err
	}

	if action.ActionType != WaitForDatasetAction {
		return true, errors.Errorf("Unsupported action %s", action.ActionType)
	}

	// First search if there is a dataset with the expected metadata
	results, err := ddiClient.SearchDataset(actionData.token, metadata)
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
			listing, err = ddiClient.ListDataSet(actionData.token, datasetRes.Location.InternalID,
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

	if action.ActionType != StoreRunningHPCJobFilesToDDIAction {
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
			return true, errors.Wrapf(err, "Wrong format for files patterns %s for actioType %s", filesPatternsStr, action.ActionType)
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

	// Refresh input values
	envInputs, _, err := operations.ResolveInputsWithInstances(
		ctx, deploymentID, actionData.nodeName, actionData.taskID, opStore, nil, nil)
	if err != nil {
		return false, errors.Wrapf(err, "Failed to get env inputs for %s", operationStr)
	}

	log.Printf("LOLO env inputs: %+v\n", envInputs)

	jobState := strings.ToLower(o.getValueFromEnv(jobStateEnvVar, envInputs))
	var jobDone bool
	switch jobState {
	case "initial", "creating", "created", "submitting", "submitted", "pending":
		log.Printf("LOLO Job state %s, nothing to do yet", jobState)
		return deregister, err
	case "executed", "completed", "failed", "canceled":
		log.Printf("LOLO job done with state %s", jobState)
		jobDone = true
	default:
		log.Printf("LOLO job state %s not yet done", jobState)
		jobDone = false
	}

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

	log.Printf("LOLO dataset path %s\n", datasetPath)

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

	log.Printf("LOLO stored files %+v\n", storedFiles)

	// Get details of files to be stored
	var toBeStoredFiles map[string]ToBeStoredFileInfo
	val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, actionData.nodeName, "0", toBeStoredFilesConsulAttribute)
	if err == nil && val != nil && val.RawString() != "" {
		err = json.Unmarshal([]byte(val.RawString()), &toBeStoredFiles)
		if err != nil {
			err = errors.Wrapf(err, "Failed to parse map of to be stored files %s", val.RawString())
			return true, err
		}
	}

	log.Printf("LOLO to be stored files %+v\n", toBeStoredFiles)

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

	log.Printf("LOLO file patterns %+v\n", filesPatterns)

	startDateStr := o.getValueFromEnv(jobStartDataEnvVar, envInputs)
	if startDateStr == "" {
		log.Printf("LOLO Nothing to store yet for %s %s, related HEAppE job not yet started", deploymentID, actionData.nodeName)
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
		log.Printf("LOLO Nothing to store yet for %s %s, related HEAppE job has not yet created/updated files", deploymentID, actionData.nodeName)
		return deregister, err

	}
	var changedFiles []ChangedFile
	err = json.Unmarshal([]byte(changedFilesStr), &changedFiles)
	if err != nil {
		return true, errors.Wrapf(err, "Wrong format for changed files %s for actioType %s", changedFilesStr, action.ActionType)
	}

	// Keeping only the files since job start not already stored, removing any input file added before
	// and removing files not matching the filters if any is defined
	var newFilesUpdates []ChangedFile
	layout = "2006-01-02T15:04:00Z"
	for _, changedFile := range changedFiles {
		changedTime, err := time.Parse(layout, changedFile.LastModifiedDate)
		if err != nil {
			log.Printf("LOLO Deployment %s node %s ignoring last modified date %s which has not the expected layout %s",
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
			matches, err := o.matchesFilter(changedFile.FileName, filesPatterns)
			if err != nil {
				return true, errors.Wrapf(err, "Failed to check if file %s matches filters %v", changedFile.FileName, filesPatterns)
			}
			if matches {
				newFilesUpdates = append(newFilesUpdates, changedFile)
			} else {
				log.Printf("LOLO ignoring file %s not matching patterns %+v\n", changedFile.FileName, filesPatterns)
			}
		}
	}

	log.Printf("LOLO new files updates: %+v\n", newFilesUpdates)

	// Update the maps of files to be stored
	toBeStoredUpdated := make(map[string]ToBeStoredFileInfo)
	toStore := make(map[string]ChangedFile)
	currentTime := time.Now()
	layout = "2006-01-02T15:04:05.00Z"
	currentDate := currentTime.Format(layout)
	log.Printf("LOLO current date: %+sn", currentDate)
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
				insertTime, _ := time.Parse(layout, toBeStoredFile.CandidateToStorageDate)
				duration := currentTime.Sub(insertTime)
				if duration >= elapsedDuration {
					toStore[changedFile.FileName] = changedFile
				} else {
					toBeStoredUpdated[changedFile.FileName] = toBeStoredFile
				}
			}
		} else {
			toBeStoredUpdated[changedFile.FileName] = ToBeStoredFileInfo{
				LastModifiedDate:       changedFile.LastModifiedDate,
				CandidateToStorageDate: currentDate,
			}
		}
	}

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

	for name, details := range toStore {
		sourcePath := path.Join(jobDirPath, name)
		var metadata ddi.Metadata
		requestID, err := ddiClient.SubmitHPCToDDIDataTransfer(metadata, actionData.token, sourceSystem,
			sourcePath, datasetPath, heappeJobID, taskID)
		if err != nil {
			return true, errors.Wrapf(err, "Failed to submit data transfer of %s to DDI", sourcePath)
		}

		log.Printf("LOLO Submitted request to store %s request_id %s\n", sourcePath, requestID)

		storedFiles[name] = StoredFileInfo{
			LastModifiedDate: details.LastModifiedDate,
			RequestID:        requestID,
		}
	}

	log.Printf("LOLO Files stored updated: %+v\n", storedFiles)

	// Save the new stored values
	err = deployments.SetAttributeComplexForAllInstances(ctx, deploymentID, actionData.nodeName,
		storedFilesConsulAttribute, storedFiles)
	if err != nil {
		return deregister, errors.Wrapf(err, "Failed to store %s %s %s value %+v",
			deploymentID, actionData.nodeName, storedFilesConsulAttribute, storedFiles)
	}

	// TODO check requests IDs and set deregister to true only when all request IDs are done
	deregister = jobDone
	return deregister, err
}

func (o *ActionOperator) matchesFilter(fileName string, filesPatterns []string) (bool, error) {
	for _, fPattern := range filesPatterns {
		matched, err := regexp.MatchString(fPattern, fileName)
		if err != nil {
			return false, err
		}
		if matched {
			return true, err
		}
	}

	return (len(filesPatterns) == 0), nil
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
	// Check token
	actionData.token, ok = action.Data[actionDataToken]
	if !ok {
		return actionData, errors.Errorf("Missing mandatory information token for actionType:%q", action.ActionType)
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
