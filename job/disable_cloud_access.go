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
	"strings"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/tosca"
)

// DisableCloudAccessJobExecution holds Cloud staging area access disablement job Execution properties
type DisableCloudAccessJobExecution struct {
	*DDIJobExecution
}

// Execute executes a synchronous operation
func (e *DisableCloudAccessJobExecution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Job %q", e.NodeName)
		err = e.setCloudStagingAreaAccessDetails(ctx)
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting Job %q", e.NodeName)
		// Nothing to do here
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Submitting data transfer request %q", e.NodeName)
		err = e.submitDisableCloudAccess(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to submit cloud access disablement for node %q, error %s", e.NodeName, err.Error())

		}
	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Canceling Job %q", e.NodeName)
		/*
			err = e.cancelJob(ctx)
			if err != nil {
				events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
					"Failed to cancel Job %q, error %s", e.NodeName, err.Error())

			}
		*/
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)

	default:
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)
	}

	return err
}

func (e *DisableCloudAccessJobExecution) submitDisableCloudAccess(ctx context.Context) error {

	ddiClient, err := getDDIClient(ctx, e.Cfg, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	ipAddress := e.GetValueFromEnvInputs(ipAddressEnvVar)
	if ipAddress == "" {
		return errors.Errorf("Failed to get ip address for which to enable access to Cloud staging area")
	}

	token, err := e.AAIClient.GetAccessToken()
	if err != nil {
		return err
	}

	requestID, err := ddiClient.SubmitDisableCloudAccess(token, ipAddress)
	if err != nil {
		return err
	}

	// Store the request id
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		requestIDConsulAttribute, requestID)
	if err != nil {
		return errors.Wrapf(err, "Request %s submitted, but failed to store this request id", requestID)
	}

	return err
}
