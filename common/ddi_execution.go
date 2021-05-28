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
	"encoding/json"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/laurentganne/yorc-ddi-plugin/ddi"
	"github.com/lexis-project/yorcoidc"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/helper/consulutil"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/prov/operations"
	"github.com/ystia/yorc/v4/storage"
	storageTypes "github.com/ystia/yorc/v4/storage/types"
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
	DatasetInfoNumberOfSmallFiles = "number_of_small_files"
	// DatasetInfoLocations is an attribute providing the size in bytes of a dataset
	DatasetInfoSize = "size"
	// AccessTokenConsulAttribute is the access token attribute of a job stored in consul
	AccessTokenConsulAttribute               = "access_token"
	locationAAIURL                           = "aai_url"
	locationAAIClientID                      = "aai_client_id"
	locationAAIClientSecret                  = "aai_client_secret"
	locationAAIRealm                         = "aai_realm"
	associatedComputeInstanceRequirementName = "os"
	hostingComputeInstanceRequirementName    = "host"
	cloudStagingAreaAccessCapability         = "cloud_staging_area_access"
	ddiAccessCapability                      = "ddi_access"
	jobChangedFilesEnvVar                    = "JOB_CHANGED_FILES"
	jobStartDateEnvVar                       = "JOB_START_DATE"
	neededFilesPatternsEnvVar                = "NEEDED_FILES_PATTERNS"

	osCapability        = "tosca.capabilities.OperatingSystem"
	heappeJobCapability = "org.lexis.common.heappe.capabilities.HeappeJob"
)

// ChangedFile holds properties of a file created/updated by a job
type ChangedFile struct {
	FileName         string
	LastModifiedDate string
}

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
	AAIClient      yorcoidc.Client
}

// GetStoredNodeTemplate returns the description of a node stored by Yorc
func GetStoredNodeTemplate(ctx context.Context, deploymentID, nodeName string) (*tosca.NodeTemplate, error) {
	node := new(tosca.NodeTemplate)
	nodePath := path.Join(consulutil.DeploymentKVPrefix, deploymentID, "topology", "nodes", nodeName)
	found, err := storage.GetStore(storageTypes.StoreTypeDeployment).Get(nodePath, node)
	if !found {
		err = errors.Errorf("No such node %s in deployment %s", nodeName, deploymentID)
	}
	return node, err
}

// StoreNodeTemplate stores a node template in Yorc
func StoreNodeTemplate(ctx context.Context, deploymentID, nodeName string, nodeTemplate *tosca.NodeTemplate) error {
	nodePrefix := path.Join(consulutil.DeploymentKVPrefix, deploymentID, "topology", "nodes", nodeName)
	return storage.GetStore(storageTypes.StoreTypeDeployment).Set(ctx, nodePrefix, nodeTemplate)
}

// Check if a string matches one of the filters
func MatchesFilter(fileName string, filesPatterns []string) (bool, error) {
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

// GetBooleanValueFromEnvInputs returns a boolean from environment variables provided in input
func (e *DDIExecution) GetBooleanValueFromEnvInputs(envVar string) bool {

	val := e.GetValueFromEnvInputs(envVar)
	res, _ := strconv.ParseBool(val)
	return res
}

// GetDDILocationNameFromComputeLocationName gets the DDI location name
// for the location on which the associated compute instance is running if any
func (e *DDIExecution) GetDDILocationNameFromInfrastructureLocation(ctx context.Context,
	infraLocation string) (string, error) {

	var locationName string
	locationMgr, err := locations.GetManager(e.Cfg)
	if err != nil {
		return locationName, err
	}
	// Convention: the first section of location identify the datacenter
	dcID := strings.ToLower(strings.SplitN(infraLocation, "_", 2)[0])
	locations, err := locationMgr.GetLocations()
	if err != nil {
		return locationName, err
	}

	for _, loc := range locations {
		if loc.Type == DDIInfrastructureType && strings.HasPrefix(strings.ToLower(loc.Name), dcID) {
			return loc.Name, err
		}
	}

	return locationName, err
}

// SetLocationFromAssociatedCloudInstance sets the location of this component
// according to an associated compute instance location
func (e *DDIExecution) SetLocationFromAssociatedCloudInstance(ctx context.Context) (string, error) {
	return e.setLocationFromAssociatedTarget(ctx, osCapability)
}

// SetLocationFromAssociatedHPCJob sets the location of this component
// according to an associated HPC location
func (e *DDIExecution) SetLocationFromAssociatedHPCJob(ctx context.Context) (string, error) {
	return e.setLocationFromAssociatedTarget(ctx, heappeJobCapability)
}

// GetHPCJobChangedFilesSinceStartup gets from evironment the list of files modified by a job since
// its startup
func (e *DDIExecution) GetHPCJobChangedFilesSinceStartup(ctx context.Context) ([]ChangedFile, error) {
	var changedFiles []ChangedFile
	startDateStr := e.GetValueFromEnvInputs(jobStartDateEnvVar)
	if startDateStr == "" {
		log.Debugf("No changed files for job associated to %s %s, job not yet started", e.DeploymentID, e.NodeName)
		return changedFiles, nil
	}

	// Get list of files patterns to take into account if any
	filesPatternsStr := e.GetValueFromEnvInputs(neededFilesPatternsEnvVar)
	var filesPatterns []string
	if filesPatternsStr != "" {
		err := json.Unmarshal([]byte(filesPatternsStr), &filesPatterns)
		if err != nil {
			return changedFiles, errors.Wrapf(err, "Wrong format for files patterns %s for node %s", filesPatternsStr, e.NodeName)
		}

	}

	layout := "2006-01-02T15:04:05"
	startTime, err := time.Parse(layout, startDateStr)
	if err != nil {
		err = errors.Wrapf(err, "Node %s failed to parse job start time %s, expected layout like %s", e.NodeName, startDateStr, layout)
		return changedFiles, err
	}
	// The job has started
	// Getting the list of files and keeping only those created/updated after the start date
	changedFilesStr := e.GetValueFromEnvInputs(jobChangedFilesEnvVar)

	if changedFilesStr == "" {
		log.Debugf("Nothing to store yet for %s %s, related HEAppE job has not yet created/updated files", e.DeploymentID, e.NodeName)
		return changedFiles, err

	}
	err = json.Unmarshal([]byte(changedFilesStr), &changedFiles)
	if err != nil {
		return changedFiles, errors.Wrapf(err, "Wrong format for changed files %s for job associated to %s", changedFilesStr, e.NodeName)
	}

	// Keeping only the files since job start not already stored, removing any input file added before
	// and removing files not matching the filters if any is defined
	var newFilesUpdates []ChangedFile
	layout = "2006-01-02T15:04:00Z"
	for _, changedFile := range changedFiles {
		changedTime, err := time.Parse(layout, changedFile.LastModifiedDate)
		if err != nil {
			log.Debugf("Deployment %s node %s ignoring last modified date %s which has not the expected layout %s",
				e.DeploymentID, e.NodeName, changedFile.LastModifiedDate, layout)
			continue
		}

		if startTime.Before(changedTime) {
			matches, err := MatchesFilter(changedFile.FileName, filesPatterns)
			if err != nil {
				return newFilesUpdates, errors.Wrapf(err, "Failed to check if file %s matches filters %v", changedFile.FileName, filesPatterns)
			}
			if matches {
				newFilesUpdates = append(newFilesUpdates, changedFile)
			} else {
				log.Debugf("ignoring file %s not matching patterns %+v\n", changedFile.FileName, filesPatterns)
			}
		}
	}

	return newFilesUpdates, err

}

// setLocationFromAssociatedTarget sets the location of this component
// according to an associated target
func (e *DDIExecution) setLocationFromAssociatedTarget(ctx context.Context, targetCapability string) (string, error) {
	var locationName string
	nodeTemplate, err := GetStoredNodeTemplate(ctx, e.DeploymentID, e.NodeName)
	if err != nil {
		return locationName, err
	}

	log.Printf("DEBUG got location for target associated to %s with cap %s\n", e.NodeName, targetCapability)
	// Get the associated target node name if any
	var targetNodeName string
	for _, nodeReq := range nodeTemplate.Requirements {
		for _, reqAssignment := range nodeReq {
			if reqAssignment.Capability == targetCapability {
				log.Printf("DEBUG found target %s associated to %s\n", reqAssignment.Node, e.NodeName)
				targetNodeName = reqAssignment.Node
				break
			} else {
				log.Printf("DEBUG not target %s associated to %s through %s\n", reqAssignment.Node, e.NodeName, reqAssignment.Capability)
			}
		}
	}
	if targetNodeName == "" {
		return locationName, err
	}

	// Get the target location
	targetNodeTemplate, err := GetStoredNodeTemplate(ctx, e.DeploymentID, targetNodeName)
	if err != nil {
		return locationName, err
	}
	var targetLocationName string
	if targetNodeTemplate.Metadata != nil {
		log.Printf("DEBUG target node %s metadata : %+v\n", targetNodeName, targetNodeTemplate.Metadata)
		targetLocationName = targetNodeTemplate.Metadata[tosca.MetadataLocationNameKey]
	}
	if targetLocationName == "" {
		return locationName, err
	}

	// Get the corresponding DDI location
	locationName, err = e.GetDDILocationNameFromInfrastructureLocation(ctx, targetLocationName)
	if err != nil || locationName == "" {
		return locationName, err
	}

	// Store the location name in this node template metadata
	if nodeTemplate.Metadata == nil {
		nodeTemplate.Metadata = make(map[string]string)
	}
	nodeTemplate.Metadata[tosca.MetadataLocationNameKey] = locationName
	// Location is now changed for this node template, storing it
	err = StoreNodeTemplate(ctx, e.DeploymentID, e.NodeName, nodeTemplate)
	return locationName, err
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
		cloudStagingAreaAccessCapability, "staging_area_name", cloudAreaProps.Name)
	if err != nil {
		return err
	}
	err = deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
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
		"staging_area_name", cloudAreaProps.Name)
	if err != nil {
		return err
	}
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
		ddiAccessCapability, "dataset_url", ddiClient.GetDatasetURL())
	if err != nil {
		return err
	}
	err = deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
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
		"dataset_url", ddiClient.GetDatasetURL())
	if err != nil {
		return err
	}
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
	ddiClient, _, _, err := e.GetDDIClientFromRequirement(ctx, associatedComputeInstanceRequirementName)
	return ddiClient, err
}

// GetDDIClientFromHostingComputeLocation gets a DDI client corresponding to the location
// on which the hosting compute instance is running
func (e *DDIExecution) GetDDIClientFromHostingComputeLocation(ctx context.Context) (ddi.Client, config.DynamicMap, error) {
	ddiClient, locationProps, _, err := e.GetDDIClientFromRequirement(ctx, hostingComputeInstanceRequirementName)
	return ddiClient, locationProps, err
}

// GetDDIClientFromRequirement gets a DDI client and location properties corresponding to the location
// on which the associated compute instance is running
func (e *DDIExecution) GetDDIClientFromRequirement(ctx context.Context, requirementName string) (ddi.Client, config.DynamicMap, string, error) {
	// First get the associated compute node
	targetNodeName, err := deployments.GetTargetNodeForRequirementByName(ctx,
		e.DeploymentID, e.NodeName, requirementName)
	if err != nil {
		return nil, nil, targetNodeName, err
	}

	locationMgr, err := locations.GetManager(e.Cfg)
	if err != nil {
		return nil, nil, targetNodeName, err
	}

	var locationProps config.DynamicMap
	found, locationName, err := deployments.GetNodeMetadata(ctx, e.DeploymentID,
		targetNodeName, tosca.MetadataLocationNameKey)
	if err != nil {
		return nil, nil, targetNodeName, err
	}

	if found {
		locationProps, err = e.GetDDILocationFromComputeLocation(ctx, locationMgr, locationName)
	} else {
		locationProps, err = locationMgr.GetLocationPropertiesForNode(ctx, e.DeploymentID,
			e.NodeName, DDIInfrastructureType)
	}

	if err != nil {
		return nil, nil, targetNodeName, err
	}

	var refreshTokenFunc ddi.RefreshTokenFunc = func() (string, error) {
		accessToken, _, err := RefreshToken(ctx, locationProps, e.DeploymentID)
		return accessToken, err
	}
	ddiClient, err := ddi.GetClient(locationProps, refreshTokenFunc)
	return ddiClient, locationProps, targetNodeName, err

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
		if loc.Type == DDIInfrastructureType && strings.HasPrefix(strings.ToLower(loc.Name), dcID) {
			locationProps, err := locationMgr.GetLocationProperties(loc.Name, DDIInfrastructureType)
			return locationProps, err
		}
	}

	// No such location found, returning the default location
	locationProps, err = locationMgr.GetLocationPropertiesForNode(ctx,
		e.DeploymentID, e.NodeName, DDIInfrastructureType)
	return locationProps, err

}

// GetAccessToken returns the access token for this dpeloyment
func GetAccessToken(ctx context.Context, cfg config.Configuration, deploymentID, nodeName string) (string, error) {
	locationMgr, err := locations.GetManager(cfg)
	if err != nil {
		return "", err
	}
	locationProps, err := locationMgr.GetLocationPropertiesForNode(ctx,
		deploymentID, nodeName, DDIInfrastructureType)
	if err != nil {
		return "", err
	}

	aaiClient := GetAAIClient(deploymentID, locationProps)

	return aaiClient.GetAccessToken()

}

// GetAAIClient returns the AAI client for a given location
func GetAAIClient(deploymentID string, locationProps config.DynamicMap) yorcoidc.Client {
	url := locationProps.GetString(locationAAIURL)
	clientID := locationProps.GetString(locationAAIClientID)
	clientSecret := locationProps.GetString(locationAAIClientSecret)
	realm := locationProps.GetString(locationAAIRealm)
	return yorcoidc.GetClient(deploymentID, url, clientID, clientSecret, realm)
}

// RefreshToken refreshes an access token
func RefreshToken(ctx context.Context, locationProps config.DynamicMap, deploymentID string) (string, string, error) {

	aaiClient := GetAAIClient(deploymentID, locationProps)
	// Getting an AAI client to check token validity
	accessToken, newRefreshToken, err := aaiClient.RefreshToken(ctx)
	if err != nil {
		refreshToken, _ := aaiClient.GetRefreshToken()
		log.Printf("ERROR %s attempting to refresh token %s\n", err.Error(), refreshToken)
		return accessToken, newRefreshToken, errors.Wrapf(err, "Failed to refresh token for orchestrator")
	}

	return accessToken, newRefreshToken, err
}
