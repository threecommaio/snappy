package snappy

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

func parseYamlFile(filename string) (map[string]interface{}, error) {
	if data, err := ioutil.ReadFile(filename); err == nil {
		m := make(map[string]interface{})
		if err := yaml.Unmarshal([]byte(data), &m); err != nil {
			return nil, err
		}
		return m, nil
	} else {
		return nil, err
	}
}
