package etly

import (
	"hash/fnv"
	"net/url"
	"path"
	"strings"
	"time"

	"io"

	"fmt"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
)

const timeVariableExpr = "<dateFormat:"
const modeVarableExpr = "<mod:"

var jsonDecoderFactory = toolbox.NewJSONDecoderFactory()
var jsonEncoderFactory = toolbox.NewJSONEncoderFactory()

func expandDateExpressionIfPresent(text string, sourceTime *time.Time) string {
	for j := 0; j < len(text); j++ {
		matchingExpression, dateFormat := getTimeVariableIfPresent(text)
		if matchingExpression == "" {
			break
		}
		text = expandDateFormatExprIfPresent(text, matchingExpression, dateFormat, sourceTime)
	}
	return text
}

func expandModExpressionIfPresent(text string, hash int) string {
	for j := 0; j < len(text); j++ {
		matchingExpression, mod := getModVariableIfPresent(text)
		if matchingExpression == "" {
			break
		}

		if strings.Contains(text, matchingExpression) {
			var value = toolbox.AsString(hash % mod)
			text = strings.Replace(text, matchingExpression, value, len(text))
		}
		return text
	}
	return text
}

func expandDateFormatExprIfPresent(text, matchingExpr, dateFormat string, sourceTime *time.Time) string {
	if strings.Contains(text, matchingExpr) {
		var value = sourceTime.Format(toolbox.DateFormatToLayout(dateFormat))
		text = strings.Replace(text, matchingExpr, value, len(text))
	}
	return text
}

func getTimeVariableIfPresent(text string) (string, string) {
	timeMatchIndex := strings.Index(text, timeVariableExpr)
	if timeMatchIndex == -1 {
		return "", ""
	}
	timeMatchIndex += len(timeVariableExpr)
	var timeFormat = ""
	for j := 0; j < 10; j++ {
		var aChar = text[timeMatchIndex+j : timeMatchIndex+j+1]
		if aChar != ">" {
			timeFormat += aChar
		} else {
			break
		}
	}
	return timeVariableExpr + timeFormat + ">", timeFormat
}

func getModVariableIfPresent(text string) (string, int) {
	modMatchingIndex := strings.Index(text, modeVarableExpr)
	if modMatchingIndex == -1 {
		return "", 0
	}

	modMatchingIndex += len(modeVarableExpr)
	var mod = ""
	for j := 0; j < 10; j++ {
		if !(modMatchingIndex+j+1 < len(text)) {
			break
		}
		var aChar = text[modMatchingIndex+j : modMatchingIndex+j+1]
		if aChar != ">" {
			mod += aChar
		} else {
			break
		}
	}
	return modeVarableExpr + mod + ">", toolbox.AsInt(mod)
}

func GetCurrentWorkingDir() string {
	file, _, _ := toolbox.CallerInfo(2)
	parent, _ := path.Split(file)
	return parent
}
func expandCurrentWorkingDirectory(text string) string {
	if strings.Contains(text, "<pwd>") {
		text = strings.Replace(text, "<pwd>", GetCurrentWorkingDir(), len(text))
	}
	return text
}

func extractFileNameFromURL(URL string) string {
	parsedURL, err := url.Parse(URL)
	if err != nil {
		return ""
	}
	_, file := path.Split(parsedURL.Path)
	return file
}

func hash(text string) int {
	h := fnv.New64()
	h.Write([]byte(text))
	result := int(h.Sum64())
	if result < 0 {
		return result * -1
	}
	return result
}

func decodeJSONTarget(reader io.Reader, target interface{}) error {
	var factory toolbox.DecoderFactory
	if _, ok := target.(toolbox.UnMarshaler); ok {
		factory = toolbox.NewUnMarshalerDecoderFactory()
	} else {
		factory = jsonDecoderFactory
	}
	return factory.Create(reader).Decode(target)
}

func encodeJSONSource(writer io.Writer, target interface{}) error {
	var factory toolbox.EncoderFactory
	if _, ok := target.(toolbox.Marshaler); ok {
		factory = toolbox.NewMarshalerEncoderFactory()
	} else {
		factory = jsonEncoderFactory
	}
	return factory.Create(writer).Encode(target)
}

func appendContentObject(storageService storage.Service, folderUrl string, collection *[]storage.Object) error {
	storageObjects, err := storageService.List(folderUrl)
	if err != nil {
		return err
	}
	for _, objectStorage := range storageObjects {
		if objectStorage.IsFolder() {
			if objectStorage.URL() != folderUrl {
				err = appendContentObject(storageService, objectStorage.URL(), collection)
				if err != nil {
					return err
				}
			}
		} else {
			*collection = append(*collection, objectStorage)
		}
	}
	return nil
}

func buildVariableMasterServiceMap(variableExtractionRules []*VariableExtraction, source storage.Object) (map[string]string, error) {
	var result = make(map[string]string)
	for _, variableExtraction := range variableExtractionRules {
		var value = ""
		switch strings.ToLower(variableExtraction.Source) {
		case "sourceurl":

			compiledExpression, err := compileRegExpr(variableExtraction.RegExpr)

			if err != nil {
				return nil, fmt.Errorf("Failed to build variable - unable to compile expr: %v due to %v", variableExtraction.RegExpr, err)
			}

			if compiledExpression.MatchString(source.URL()) {
				matched := compiledExpression.FindStringSubmatch(source.URL())
				value = matched[1]
			}
			result[variableExtraction.Name] = value
		case "source", "target":
			//do nothing
		default:
			return nil, fmt.Errorf("Unsupported source: %v", variableExtraction.Source)

		}
	}
	return result, nil
}

func buildVariableWorkerServiceMap(variableExtractionRules []*VariableExtraction, source, target interface{}) (map[string]string, error) {
	var result = make(map[string]string)
	for _, variableExtraction := range variableExtractionRules {

		var value = ""
		switch strings.ToLower(variableExtraction.Source) {
		case "sourceurl":
			//do nothing
		case "source":
			provider, err := NewVariableProviderRegistry().Get(variableExtraction.Provider)
			if err != nil {
				return nil, err
			}
			value = provider(source)

		case "target":
			provider, err := NewVariableProviderRegistry().Get(variableExtraction.Provider)
			if err != nil {
				return nil, err
			}
			value = provider(target)

		default:
			return nil, fmt.Errorf("Unsupported source: %v", variableExtraction.Source)

		}
		result[variableExtraction.Name] = value

	}
	return result, nil
}

func expandVaiables(text string, variables map[string]string) string {
	for k, v := range variables {
		if strings.Contains(text, k) {
			text = strings.Replace(text, k, v, -1)
		}
	}
	return text
}
