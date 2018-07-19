package snappy

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

func parseYamlFile(filename string) (map[string]interface{}, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	m := make(map[string]interface{})
	err = yaml.Unmarshal([]byte(data), &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
