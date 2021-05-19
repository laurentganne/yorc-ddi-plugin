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
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/tosca"
)

// DDIRuntimeToCloudExecution holds the properties of a data transfer from DDO to cloud
// of a dataset (or files in this dataset) determined at runtime
type DDIRuntimeToCloudExecution struct {
	*DDIJobExecution
}

// Execute executes a synchronous operation
func (e *DDIRuntimeToCloudExecution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Job %q", e.NodeName)
		var locationName string
		locationName, err = e.SetLocationFromAssociatedCloudInstance(ctx)
		if err != nil {
			return err
		}
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Location for %s is %s", e.NodeName, locationName)
		err = e.setCloudStagingAreaAccessDetails(ctx)
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting Job %q", e.NodeName)
		// Nothing to do here
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Submitting data transfer request %q", e.NodeName)
		err = e.submitDataTransferRequest(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to submit data transfer for node %q, error %s", e.NodeName, err.Error())

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

func (e *DDIRuntimeToCloudExecution) submitDataTransferRequest(ctx context.Context) error {

	ddiClient, err := getDDIClient(ctx, e.Cfg, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	sourceDatasetPath := e.GetValueFromEnvInputs(ddiDatasetPathEnvVar)
	if sourceDatasetPath == "" {
		return errors.Errorf("Failed to get path of dataset to transfer from DDI")
	}

	var sourceFilePath string
	filePattern := e.GetValueFromEnvInputs(filePatternEnvVar)
	if filePattern != "" {
		// Find a file matching this pattern
		var sourceFilePaths []string
		sourceFilePathsStr := e.GetValueFromEnvInputs(ddiDatasetFilePathsEnvVar)
		if sourceFilePathsStr != "" {
			err = json.Unmarshal([]byte(sourceFilePathsStr), &sourceFilePaths)
			if err != nil {
				return errors.Wrapf(err, "Wrong format for lsit o ffile paths %q for deployment %s node %s",
					sourceFilePathsStr, e.DeploymentID, e.NodeName)
			}
		}

		if len(sourceFilePaths) == 0 {
			return errors.Errorf("No file paths set from associated files provider in source dataset")
		}

		for _, fpath := range sourceFilePaths {
			matched, err := regexp.MatchString(filePattern, fpath)
			if err != nil {
				return errors.Wrapf(err, "Failed to find matching pattern %s in source files", filePattern)
			}
			if matched {
				sourceFilePath = fpath
				break
			}
		}

		if sourceFilePath == "" {
			return errors.Errorf("Found no file with pattern %q in source files %+v", filePattern, sourceFilePaths)
		}
	}

	destPath := e.GetValueFromEnvInputs(cloudStagingAreaDatasetPathEnvVar)
	timeStampStr := e.GetValueFromEnvInputs(timestampCloudStagingAreaDirEnvVar)
	addTimestamp, _ := strconv.ParseBool(timeStampStr)
	if addTimestamp {
		destPath = fmt.Sprintf("%s_%d", destPath, time.Now().UnixNano()/1000000)
	}

	var sourcePath string
	var cloudAreaDirectoryPath string
	if sourceFilePath != "" {
		sourcePath = sourceFilePath
		cloudAreaDirectoryPath = destPath
		fileName := path.Base(sourcePath)
		err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
			fileNameConsulAttribute, fileName)
		if err != nil {
			return errors.Wrapf(err, "Failed to store %s %s %s value %s", e.DeploymentID, e.NodeName, fileNameConsulAttribute, fileName)
		}
	} else {
		sourcePath = sourceDatasetPath
		cloudAreaDirectoryPath = path.Join(destPath, path.Base(sourceDatasetPath))
	}

	metadata, err := e.getMetadata(ctx)
	if err != nil {
		return err
	}

	token, err := e.AAIClient.GetAccessToken()
	if err != nil {
		return err
	}

	// Check encryption/compression settings
	decrypt := "no"
	if e.GetBooleanValueFromEnvInputs(decryptEnvVar) {
		decrypt = "yes"
	}
	uncompress := "no"
	if e.GetBooleanValueFromEnvInputs(uncompressEnvVar) {
		uncompress = "yes"
	}

	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
		"Submitting data transfer request for %s source %s path %s, destination %s path %s, decrypt %s, uncompress %s",
		e.NodeName, ddiClient.GetDDIAreaName(), sourcePath, ddiClient.GetCloudStagingAreaName(), destPath, decrypt, uncompress)

	requestID, err := ddiClient.SubmitDDIToCloudDataTransfer(metadata, token, sourcePath, destPath, decrypt, uncompress)
	if err != nil {
		return err
	}

	// Store the request id
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		requestIDConsulAttribute, requestID)
	if err != nil {
		return errors.Wrapf(err, "Request %s submitted, but failed to store this request id", requestID)
	}

	// Store the staging area name
	stagingAreaName := ddiClient.GetCloudStagingAreaName()
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		stagingAreaNameConsulAttribute, stagingAreaName)
	if err != nil {
		return errors.Wrapf(err, "Request %s submitted, but failed to store the staging area name %s", requestID, stagingAreaName)
	}
	err = deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		dataTransferCapability, stagingAreaNameConsulAttribute, stagingAreaName)
	if err != nil {
		return errors.Wrapf(err, "Failed to store staging area name capability attribute value %s", stagingAreaName)
	}

	// Store the staging area directory path
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		stagingAreaPathConsulAttribute, cloudAreaDirectoryPath)
	if err != nil {
		return errors.Wrapf(err, "Request %s submitted, but failed to store the staging area directory path %s", requestID, cloudAreaDirectoryPath)
	}
	err = deployments.SetCapabilityAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		dataTransferCapability, stagingAreaPathConsulAttribute, cloudAreaDirectoryPath)
	if err != nil {
		err = errors.Wrapf(err, "Failed to store cloud staging area path capability attribute value %s", cloudAreaDirectoryPath)
	}

	return err
}
