package config

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

func decodeConfigYAML(raw []byte, cfg *Config) error {
	if err := validateUnsignedIntegerYAML(raw); err != nil {
		return err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	return decoder.Decode(cfg)
}

func validateUnsignedIntegerYAML(raw []byte) error {
	// yaml.v3 otherwise accepts floating-point scalars for unsigned Go fields and truncates them.
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return err
	}
	if len(document.Content) == 0 {
		return nil
	}
	return validateUnsignedIntegerNode(document.Content[0], reflect.TypeFor[Config](), "")
}

func validateUnsignedIntegerNode(node *yaml.Node, target reflect.Type, path string) error {
	for node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	for target.Kind() == reflect.Pointer {
		if node.Tag == "!!null" {
			return nil
		}
		target = target.Elem()
	}

	switch target.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
			return fmt.Errorf("%s must be an integer", path)
		}
		if strings.HasPrefix(node.Value, "-") {
			return fmt.Errorf("%s must be a non-negative integer", path)
		}
	case reflect.Struct:
		return validateUnsignedIntegerStruct(node, target, path)
	case reflect.Slice, reflect.Array:
		if node.Kind != yaml.SequenceNode {
			return nil
		}
		for idx, item := range node.Content {
			if err := validateUnsignedIntegerNode(item, target.Elem(), fmt.Sprintf("%s[%d]", path, idx)); err != nil {
				return err
			}
		}
	case reflect.Map:
		if node.Kind != yaml.MappingNode {
			return nil
		}
		for idx := 0; idx+1 < len(node.Content); idx += 2 {
			key := node.Content[idx]
			value := node.Content[idx+1]
			if err := validateUnsignedIntegerNode(value, target.Elem(), joinYAMLPath(path, key.Value)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateUnsignedIntegerStruct(node *yaml.Node, target reflect.Type, path string) error {
	if node.Kind != yaml.MappingNode {
		return nil
	}
	values := make(map[string]*yaml.Node, len(node.Content)/2)
	for idx := 0; idx+1 < len(node.Content); idx += 2 {
		key := node.Content[idx]
		if key.Kind == yaml.ScalarNode {
			values[key.Value] = node.Content[idx+1]
		}
	}

	for field := range target.Fields() {
		if !field.IsExported() {
			continue
		}
		name, inline, skip := yamlField(field)
		if skip {
			continue
		}
		if inline {
			if err := validateUnsignedIntegerNode(node, field.Type, path); err != nil {
				return err
			}
			continue
		}
		value, ok := values[name]
		if !ok {
			continue
		}
		if err := validateUnsignedIntegerNode(value, field.Type, joinYAMLPath(path, name)); err != nil {
			return err
		}
	}
	return nil
}

func yamlField(field reflect.StructField) (name string, inline, skip bool) {
	parts := strings.Split(field.Tag.Get("yaml"), ",")
	name = parts[0]
	if name == "-" {
		return "", false, true
	}
	if name == "" {
		name = strings.ToLower(field.Name)
	}
	for _, option := range parts[1:] {
		if option == "inline" {
			inline = true
		}
	}
	return name, inline, false
}

func joinYAMLPath(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}
