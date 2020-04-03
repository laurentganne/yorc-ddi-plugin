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

package standard

import (
	"context"
	"strings"
	"time"

	"github.com/laurentganne/yorc-ddi-plugin/v1/common"
	"github.com/laurentganne/yorc-ddi-plugin/v1/ddi"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tosca"
)

const (
	associatedComputeInstanceRequirementName = "host"
)

// CloudStagingAreaExecution holds Cloud staging area Execution properties
type CloudStagingAreaExecution struct {
	*common.DDIExecution
}

// ExecuteAsync is not supported here
func (e *CloudStagingAreaExecution) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	return nil, 0, errors.Errorf("Unsupported asynchronous operation %s", e.Operation.Name)
}

// Execute executes a synchronous operation
func (e *CloudStagingAreaExecution) Execute(ctx context.Context) error {

	var err error
	var ddiClient ddi.Client
	switch strings.ToLower(e.Operation.Name) {
	case "install", "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating %q", e.NodeName)
		ddiClient, err = e.getDDIClientFromAssociatedComputeLocation(ctx)
		if err != nil {
			return err
		}
		err = e.SetCloudStagingAreaAccessCapabilityAttributes(ctx, ddiClient)
	case "uninstall", "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting %q", e.NodeName)
		// Nothing to do here
	case "standard.start", "standard.stop":
		// Nothing to do
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Executing operation %s on node %q", e.Operation.Name, e.NodeName)
	case tosca.RunnableSubmitOperationName, tosca.RunnableCancelOperationName:
		err = errors.Errorf("Unsupported operation %s", e.Operation.Name)
	default:
		err = errors.Errorf("Unsupported operation %s", e.Operation.Name)
	}

	return err
}

func (e *CloudStagingAreaExecution) getDDIClientFromAssociatedComputeLocation(ctx context.Context) (ddi.Client, error) {
	// First get the associated compute node
	computeNodeName, err := deployments.GetTargetNodeForRequirementByName(ctx,
		e.DeploymentID, e.NodeName, associatedComputeInstanceRequirementName)
	if err != nil {
		return nil, err
	}

	locationMgr, err := locations.GetManager(e.Cfg)
	if err != nil {
		return nil, err
	}

	var locationProps config.DynamicMap
	found, locationName, err := deployments.GetNodeMetadata(ctx, e.DeploymentID,
		computeNodeName, tosca.MetadataLocationNameKey)
	if err != nil {
		return nil, err
	}

	if found {
		locationProps, err = e.getDDILocationFromComputeLocation(ctx, locationMgr, locationName)
	} else {
		locationProps, err = locationMgr.GetLocationPropertiesForNode(ctx, e.DeploymentID,
			e.NodeName, common.DDIInfrastructureType)
	}

	if err != nil {
		return nil, err
	}

	return ddi.GetClient(locationProps)

}
func (e *CloudStagingAreaExecution) getDDILocationFromComputeLocation(ctx context.Context,
	locationMgr locations.Manager, computeLocation string) (config.DynamicMap, error) {

	var locationProps config.DynamicMap

	// Convention: the last 3 letters of this location identiy the datacenter
	dcID := computeLocation[(len(computeLocation) - 3):]
	locations, err := locationMgr.GetLocations()
	if err != nil {
		return locationProps, err
	}

	for _, loc := range locations {
		if loc.Type == common.DDIInfrastructureType && strings.HasSuffix(computeLocation, dcID) {
			locationProps, err := locationMgr.GetLocationProperties(loc.Name, common.DDIInfrastructureType)
			return locationProps, err
		}
	}

	// No such location found, returning the default location
	locationProps, err = locationMgr.GetLocationPropertiesForNode(ctx,
		e.DeploymentID, e.NodeName, common.DDIInfrastructureType)
	return locationProps, err

}
