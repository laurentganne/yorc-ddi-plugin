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

	"github.com/hashicorp/consul/api"
	"github.com/laurentganne/yorc-ddi-plugin/v1/ddi"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/prov/operations"
)

const (
	// DDIInfrastructureType is the DDI location infrastructure type
	DDIInfrastructureType            = "ddi"
	cloudStagingAreaAccessCapability = "cloud_staging_area_access"
	ddiAccessCapability              = "ddi_access"
)

// DDIExecution holds DDI Execution properties
type DDIExecution struct {
	KV             *api.KV
	Cfg            config.Configuration
	DeploymentID   string
	TaskID         string
	NodeName       string
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
