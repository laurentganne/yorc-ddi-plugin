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

package common

import (
	"context"
	"strings"

	"github.com/hashicorp/consul/api"
	"github.com/laurentganne/yorc-ddi-plugin/v1/ddi"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/prov/operations"
	"github.com/ystia/yorc/v4/tosca"
)

const (
	// DDIInfrastructureType is the DDI location infrastructure type
	DDIInfrastructureType = "ddi"
	// DatasetInfoCapability is the capability of a component providing info on a dataset
	DatasetInfoCapability = "dataset_info"
	// DatasetInfoLocations is an attribute providing the list of DDI areas where the
	// dataset is available
	DatasetInfoLocations = "locations"
	// DatasetInfoLocations is an attribute providing the number of files in a dataset
	DatasetInfoNumberOfFiles = "number_of_files"
	// DatasetInfoLocations is an attribute providing the number of small files in a dataset (<= 32MB)
	DatasetInfoNumberOfSmallFiles = "number_of_files"
	// DatasetInfoLocations is an attribute providing the size in bytes of a dataset
	DatasetInfoSize                          = "size"
	associatedComputeInstanceRequirementName = "os"
	cloudStagingAreaAccessCapability         = "cloud_staging_area_access"
	ddiAccessCapability                      = "ddi_access"
)

// DDIExecution holds DDI Execution properties
type DDIExecution struct {
	KV             *api.KV
	Cfg            config.Configuration
	DeploymentID   string
	TaskID         string
	NodeName       string
	Token          string
	Operation      prov.Operation
	EnvInputs      []*operations.EnvInput
	VarInputsNames []string
}

// ResolveExecution resolves inputs before the execution of an operation
func (e *DDIExecution) ResolveExecution(ctx context.Context) error {
	return e.resolveInputs(ctx)
}

// GetValueFromEnvInputs returns a value from environment variables provided in input
func (e *DDIExecution) GetValueFromEnvInputs(envVar string) string {

	var result string
	for _, envInput := range e.EnvInputs {
		if envInput.Name == envVar {
			result = envInput.Value
			break
		}
	}
	return result

}

func (e *DDIExecution) resolveInputs(ctx context.Context) error {
	var err error
	log.Debugf("Get environment inputs for node:%q", e.NodeName)
	e.EnvInputs, e.VarInputsNames, err = operations.ResolveInputsWithInstances(
		ctx, e.DeploymentID, e.NodeName, e.TaskID, e.Operation, nil, nil)
	log.Debugf("Environment inputs: %v", e.EnvInputs)
	return err
}

// SetCloudStagingAreaAccessCapabilityAttributes sets the corresponding capability attributes
func (e *DDIExecution) SetCloudStagingAreaAccessCapabilityAttributes(ctx context.Context, ddiClient ddi.Client) error {

	cloudAreaProps := ddiClient.GetCloudStagingAreaProperties()

	err := deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		cloudStagingAreaAccessCapability, "remote_file_system", cloudAreaProps.RemoteFileSystem)
	if err != nil {
		return err
	}
	err = deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		cloudStagingAreaAccessCapability, "mount_type", cloudAreaProps.MountType)
	if err != nil {
		return err
	}
	err = deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		cloudStagingAreaAccessCapability, "mount_options", cloudAreaProps.MountOptions)
	if err != nil {
		return err
	}
	err = deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		cloudStagingAreaAccessCapability, "user_id", cloudAreaProps.UserID)
	if err != nil {
		return err
	}
	err = deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		cloudStagingAreaAccessCapability, "group_id", cloudAreaProps.GroupID)
	if err != nil {
		return err
	}

	// Adding as well node template attributes
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		"remote_file_system", cloudAreaProps.RemoteFileSystem)
	if err != nil {
		return err
	}
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		"mount_type", cloudAreaProps.MountType)
	if err != nil {
		return err
	}
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		"mount_options", cloudAreaProps.MountOptions)
	if err != nil {
		return err
	}
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		"user_id", cloudAreaProps.UserID)
	if err != nil {
		return err
	}
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		"group_id", cloudAreaProps.GroupID)
	return err
}

func (e *DDIExecution) SetDDIAccessCapabilityAttributes(ctx context.Context, ddiClient ddi.Client) error {

	err := deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		ddiAccessCapability, "staging_url", ddiClient.GetStagingURL())
	if err != nil {
		return err
	}

	err = deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		ddiAccessCapability, "sshfs_url", ddiClient.GetSshfsURL())
	if err != nil {
		return err
	}
	// Adding as well node template attributes
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		"staging_url", ddiClient.GetStagingURL())
	if err != nil {
		return err
	}
	return deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		"sshfs_url", ddiClient.GetSshfsURL())
}

func (e *DDIExecution) SetDatasetInfoCapabilityLocationsAttribute(ctx context.Context, locationNames []string) error {

	err := deployments.SetCapabilityAttributeComplexForAllInstances(ctx, e.DeploymentID, e.NodeName,
		DatasetInfoCapability, DatasetInfoLocations, locationNames)
	if err != nil {
		return err
	}

	// Adding as well node template attributes
	err = deployments.SetAttributeComplexForAllInstances(ctx, e.DeploymentID, e.NodeName,
		DatasetInfoLocations, locationNames)
	return err
}

func (e *DDIExecution) SetDatasetInfoCapabilitySizeAttribute(ctx context.Context, size string) error {

	err := deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		DatasetInfoCapability, DatasetInfoSize, size)
	if err != nil {
		return err
	}

	// Adding as well node template attributes
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		DatasetInfoSize, size)
	return err
}

func (e *DDIExecution) SetDatasetInfoCapabilityNumberOfFilesAttribute(ctx context.Context, filesNumber string) error {

	err := deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		DatasetInfoCapability, DatasetInfoNumberOfFiles, filesNumber)
	if err != nil {
		return err
	}

	// Adding as well node template attributes
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		DatasetInfoNumberOfFiles, filesNumber)
	return err
}

func (e *DDIExecution) SetDatasetInfoCapabilityNumberOfSmallFilesAttribute(ctx context.Context, filesNumber string) error {

	err := deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		DatasetInfoCapability, DatasetInfoNumberOfSmallFiles, filesNumber)
	if err != nil {
		return err
	}

	// Adding as well node template attributes
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		DatasetInfoNumberOfSmallFiles, filesNumber)
	return err
}

// GetDDIClientFromAssociatedComputeLocation gets a DDI client corresponding to the location
// on which the associated compute instance is running
func (e *DDIExecution) GetDDIClientFromAssociatedComputeLocation(ctx context.Context) (ddi.Client, error) {
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
		locationProps, err = e.GetDDILocationFromComputeLocation(ctx, locationMgr, locationName)
	} else {
		locationProps, err = locationMgr.GetLocationPropertiesForNode(ctx, e.DeploymentID,
			e.NodeName, DDIInfrastructureType)
	}

	if err != nil {
		return nil, err
	}

	return ddi.GetClient(locationProps)

}

// GetDDILocationFromComputeLocation gets the DDI location
// for the location on which the associated compute instance is running
func (e *DDIExecution) GetDDILocationFromComputeLocation(ctx context.Context,
	locationMgr locations.Manager, computeLocation string) (config.DynamicMap, error) {

	var locationProps config.DynamicMap

	// Convention: the first section of location identify the datacenter
	dcID := strings.ToLower(strings.SplitN(computeLocation, "_", 2)[0])
	locations, err := locationMgr.GetLocations()
	if err != nil {
		return locationProps, err
	}

	for _, loc := range locations {
		if loc.Type == DDIInfrastructureType && strings.HasSuffix(strings.ToLower(loc.Name), dcID) {
			locationProps, err := locationMgr.GetLocationProperties(loc.Name, DDIInfrastructureType)
			return locationProps, err
		}
	}

	// No such location found, returning the default location
	locationProps, err = locationMgr.GetLocationPropertiesForNode(ctx,
		e.DeploymentID, e.NodeName, DDIInfrastructureType)
	return locationProps, err

}
