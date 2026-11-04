package hosts

import (
	"encoding/csv"
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/pflag"
)

type labelSelector struct {
	literal string
	pattern *regexp.Regexp
}

func (selector labelSelector) matches(labels []string) bool {
	for _, label := range labels {
		if selector.pattern != nil {
			if selector.pattern.MatchString(label) {
				return true
			}
		} else if label == selector.literal {
			return true
		}
	}
	return false
}

func matchesAny(selectors []labelSelector, labels []string) bool {
	for _, selector := range selectors {
		if selector.matches(labels) {
			return true
		}
	}
	return false
}

func selectorBody(value string) string {
	if len(value) > 0 && (value[0] == '!' || value[0] == '&') {
		return value[1:]
	}
	return value
}

func extendedSelector(value string) bool {
	value = selectorBody(value)
	return strings.HasPrefix(value, "re:") || strings.HasPrefix(value, "literal:")
}

func compileSelectors(values []string) (include, exclude, required []labelSelector, err error) {
	for _, value := range values {
		if value == "" {
			continue
		}
		body := selectorBody(value)
		selector := labelSelector{}
		switch {
		case strings.HasPrefix(body, "literal:"):
			selector.literal = strings.TrimPrefix(body, "literal:")
		case strings.HasPrefix(body, "re:"):
			expression := strings.TrimPrefix(body, "re:")
			if expression == "" {
				return nil, nil, nil, fmt.Errorf("empty label regular expression")
			}
			selector.pattern, err = regexp.Compile(expression)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("invalid label selector %q: %w", value, err)
			}
		default:
			selector.literal = body
		}
		switch value[0] {
		case '!':
			exclude = append(exclude, selector)
		case '&':
			required = append(required, selector)
		default:
			include = append(include, selector)
		}
	}
	return include, exclude, required, nil
}

// AddLabelFlag retains each command's legacy separator for exact labels.
// Extended expressions occupy one complete -l argument, so regexp commas and
// colons remain intact. Repeat -l to combine them with other selectors.
func AddLabelFlag(flags *pflag.FlagSet, target *[]string, separator string) {
	flags.StringSliceVarP(target, "labels", "l", []string{}, "Host/label selectors; re:REGEXP enables regex, ! excludes, & requires; repeat -l for expressions")
	flag := flags.Lookup("labels")
	flag.Value = &labelFlagValue{Value: flag.Value, slice: flag.Value.(pflag.SliceValue), separator: separator}
}

type labelFlagValue struct {
	pflag.Value
	slice     pflag.SliceValue
	separator string
	changed   bool
}

func (value *labelFlagValue) Set(raw string) error {
	var fields []string
	if extendedSelector(raw) {
		fields = []string{raw}
	} else if raw != "" {
		for _, prefix := range []string{"re:", "!re:", "&re:", "literal:", "!literal:", "&literal:"} {
			if strings.Contains(raw, value.separator+prefix) {
				return fmt.Errorf("pass re: and literal: selectors as separate -l arguments")
			}
		}
		if value.separator == "," {
			var err error
			fields, err = csv.NewReader(strings.NewReader(raw)).Read()
			if err != nil {
				return err
			}
		} else {
			fields = strings.Split(raw, value.separator)
		}
		for _, field := range fields {
			if extendedSelector(field) {
				return fmt.Errorf("pass re: and literal: selectors as separate -l arguments")
			}
		}
	}
	if _, _, _, err := compileSelectors(fields); err != nil {
		return err
	}
	if !value.changed {
		if err := value.slice.Replace(fields); err != nil {
			return err
		}
	} else {
		for _, field := range fields {
			if err := value.slice.Append(field); err != nil {
				return err
			}
		}
	}
	value.changed = true
	return nil
}
