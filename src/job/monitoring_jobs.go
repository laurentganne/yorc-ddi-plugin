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
	"strings"

	"github.com/laurentganne/yorc-ddi-plugin/v1/ddi"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
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
	// WaitForDatasetAction is the action of waiting for a dataset to appear in DDI
	WaitForDatasetAction        = "wait-for-dataset"
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
	projectPathPattern          = "project/proj%x"
	datasetElementDirectoryType = "directory"
	datasetElementFileType      = "file"
)

// ActionOperator holds function allowing to execute an action
type ActionOperator struct {
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
		action.ActionType == EnableCloudAccessAction || action.ActionType == DisableCloudAccessAction {
		deregister, err = o.monitorJob(ctx, cfg, deploymentID, action)
	} else if action.ActionType == WaitForDatasetAction {
		deregister, err = o.monitorDataset(ctx, cfg, deploymentID, action)
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
	switch action.ActionType {
	case EnableCloudAccessAction:
		status, err = ddiClient.GetEnableCloudAccessRequestStatus(actionData.token, requestID)
	case DisableCloudAccessAction:
		status, err = ddiClient.GetDisableCloudAccessRequestStatus(actionData.token, requestID)
	case DataTransferAction:
		status, targetPath, err = ddiClient.GetDataTransferRequestStatus(actionData.token, requestID)
	case CloudDataDeleteAction:
		status, err = ddiClient.GetDeletionRequestStatus(actionData.token, requestID)
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
		projectPath := fmt.Sprintf(projectPathPattern, md5.Sum([]byte(datasetRes.Location.Project)))
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
