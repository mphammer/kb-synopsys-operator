package controllers

import (
	"gopkg.in/yaml.v2"
)

// Dependency Resources from YAML file
type RuntimeObjectDependency struct {
	Obj           string `yaml:"obj"`
	IsDependentOn string `yaml:"isdependenton"`
}
type RuntimeObjectDepencyYaml struct {
	Groups       map[string][]string       `yaml:"runtimeobjectsgroupings"`
	Dependencies []RuntimeObjectDependency `yaml:"runtimeobjectdependencies"`
}

func CreateInstructionManual(instructionManualLocation string) (*RuntimeObjectDepencyYaml, error) {
	// Read Dependcy YAML File into Struct
	dependencyYamlBytes, err := HttpGet(instructionManualLocation)
	if err != nil {
		return nil, err
	}

	dependencyYamlStruct := &RuntimeObjectDepencyYaml{}
	err = yaml.Unmarshal(dependencyYamlBytes, dependencyYamlStruct)
	if err != nil {
		return nil, err
	}
	return dependencyYamlStruct, nil
}
