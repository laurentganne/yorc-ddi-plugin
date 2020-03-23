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
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
)

const (
	// DataTransferAction is the action of transferring a dataset
	DataTransferAction = "transfer-request-monitoring"
	// CloudDataDeleteAction is the action of deleting a dataset from Cloud storage
	CloudDataDeleteAction     = "cloud-data-delete-monitoring"
	actionDataSessionID       = "sessionID"
	requestStatusPending      = "PENDING"
	requestStatusRunning      = "RUNNING"
	requestStatusCompleted    = "COMPLETED"
	requestStatusFailed       = "FAILED"
	requestStatusCanceled     = "CANCELED"
	actionDataOffsetKeyFormat = "%d_%d_%d"
	taskFailurePrefix         = "Task Failed, reason: "
)

type fileType int

// ActionOperator holds function allowing to execute an action
type ActionOperator struct {
}

type actionData struct {
	token     string
	requestID string
	taskID    string
	nodeName  string
}

// ExecAction allows to execute and action
func (o *ActionOperator) ExecAction(ctx context.Context, cfg config.Configuration, taskID, deploymentID string, action *prov.Action) (bool, error) {
	log.Debugf("Execute Action with ID:%q, taskID:%q, deploymentID:%q", action.ID, taskID, deploymentID)

	if action.ActionType == DataTransferAction || action.ActionType == CloudDataDeleteAction {
		deregister, err := o.monitorJob(ctx, cfg, deploymentID, action)
		if err != nil {
			// action scheduling needs to be unregistered
			return true, err
		}

		return deregister, nil
	}
	return true, errors.Errorf("Unsupported actionType %q", action.ActionType)
}

func (o *ActionOperator) monitorJob(ctx context.Context, cfg config.Configuration, deploymentID string, action *prov.Action) (bool, error) {
	var (
		err        error
		deregister bool
		ok         bool
	)

	actionData := &actionData{}
	// Check nodeName
	actionData.nodeName, ok = action.Data["nodeName"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information nodeName for actionType:%q", action.ActionType)
	}
	// Check requestID
	actionData.requestID, ok = action.Data["requestID"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information requestID for actionType:%q", action.ActionType)
	}
	// Check token
	actionData.token, ok = action.Data["token"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information token for actionType:%q", action.ActionType)
	}
	// Check taskID
	actionData.taskID, ok = action.Data["taskID"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information taskID for actionType:%q", action.ActionType)
	}

	ddiClient, err := getDDIClient(ctx, cfg, deploymentID, actionData.nodeName)
	if err != nil {
		return true, err
	}

	var status string
	switch action.ActionType {
	case DataTransferAction:
		status, err = ddiClient.GetDataTransferRequestStatus(actionData.token, actionData.requestID)
		// Nothing to do here
	case CloudDataDeleteAction:
		status, err = ddiClient.GetDeletionRequestStatus(actionData.token, actionData.requestID)
	default:
		err = errors.Errorf("Unsupported action %s", action.ActionType)
	}
	if err != nil {
		return true, err
	}

	var requestStatus string
	var errorMessage string
	switch {
	case status == "Task still in the queue, or task does not exist":
		requestStatus = requestStatusPending
	case status == "In progress":
		requestStatus = requestStatusRunning
	case status == "Transfer completed":
		requestStatus = requestStatusCompleted
	case status == "Data deleted":
		requestStatus = requestStatusCompleted
	case strings.HasPrefix(status, taskFailurePrefix):
		status = requestStatusFailed
		errorMessage = status[(len(taskFailurePrefix) - 1):]
	default:
		return true, errors.Errorf("Unexpexted :%q", action.ActionType)
	}

	previousRequestStatus, err := deployments.GetInstanceStateString(ctx, deploymentID, actionData.nodeName, "0")
	if err != nil {
		return true, errors.Wrapf(err, "failed to get instance state for request %d", actionData.requestID)
	}

	// See if monitoring must be continued and set job state if terminated
	switch requestStatus {
	case requestStatusCompleted:
		// job has been done successfully : unregister monitoring
		deregister = true
	case requestStatusPending, requestStatusRunning:
		// job's still running or its state is about to be set definitively: monitoring is keeping on (deregister stays false)
	default:
		// Other cases as FAILED, CANCELED : error is return with job state and job info is logged
		deregister = true
		// Log event containing all the slurm information

		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(fmt.Sprintf("request status: %+v", requestStatus))
		// Error to be returned
		err = errors.Errorf("Request ID %s finished unsuccessfully with status: %s, reason: %s", actionData.requestID, requestStatus, errorMessage)
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
