package generator

import (
	_ "embed"
	"fmt"
	"path"
	"strconv"
	"strings"
	"unicode"

	"github.com/fern-api/fern-go/internal/fern/ir"
	"github.com/fern-api/fern-go/internal/gospec"
)

var (
	//go:embed model/core/stringer.go
	stringerFile string
)

// WriteType writes a complete type, including all of its properties.
func (f *fileWriter) WriteType(typeDeclaration *ir.TypeDeclaration, includeRawJSON bool) error {
	visitor := &typeVisitor{
		typeName:       typeDeclaration.Name.Name.PascalCase.UnsafeName,
		baseImportPath: f.baseImportPath,
		importPath:     fernFilepathToImportPath(f.baseImportPath, typeDeclaration.Name.FernFilepath),
		writer:         f,
		includeRawJSON: includeRawJSON,
	}
	f.WriteDocs(typeDeclaration.Docs)
	return typeDeclaration.Shape.Accept(visitor)
}

// typeVisitor writes the internal properties of types (e.g. properties).
type typeVisitor struct {
	typeName       string
	baseImportPath string
	importPath     string
	writer         *fileWriter

	includeRawJSON bool
}

// Compile-time assertion.
var _ ir.TypeVisitor = (*typeVisitor)(nil)

func (t *typeVisitor) VisitAlias(alias *ir.AliasTypeDeclaration) error {
	t.writer.P("type ", t.typeName, " = ", typeReferenceToGoType(alias.AliasOf, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath, false))
	t.writer.P()
	return nil
}

func (t *typeVisitor) VisitEnum(enum *ir.EnumTypeDeclaration) error {
	// Write the enum type definition.
	t.writer.P("type ", t.typeName, " string")
	t.writer.P()

	// We first need to determine whether or not this enum requires us to
	// use its wire value (as opposed to just its enum name). This prevents
	// collisions that would otherwise occur by an equivalent pascal case
	// representation. For example,
	//
	// type Something uint8
	//
	// const (
	//   SomethingOne Something = iota + 1
	//   SomethingONE Something
	// )
	//
	var useEnumWireValue bool
	enumNames := make(map[string]struct{}, len(enum.Values))
	for _, enumValue := range enum.Values {
		enumName := enumValue.Name.Name.PascalCase.UnsafeName
		if _, ok := enumNames[enumName]; ok {
			useEnumWireValue = true
			break
		}
		enumNames[enumName] = struct{}{}
	}

	// Write all of the supported enum values in a single const block.
	t.writer.P("const (")
	for _, enumValue := range enum.Values {
		t.writer.WriteDocs(enumValue.Docs)
		enumName := t.typeName + enumValue.Name.Name.PascalCase.UnsafeName
		if useEnumWireValue {
			enumName = t.typeName + enumValue.Name.WireValue
		}
		t.writer.P(enumName, " ", t.typeName, fmt.Sprintf(" = %q", enumValue.Name.WireValue))
	}
	t.writer.P(")")
	t.writer.P()

	// Generate a constructor that can be used to validate the string value.
	t.writer.P("func New", t.typeName, "FromString(s string) (", t.typeName, ", error) {")
	t.writer.P("switch s {")
	for _, enumValue := range enum.Values {
		enumName := t.typeName + enumValue.Name.Name.PascalCase.UnsafeName
		if useEnumWireValue {
			enumName = t.typeName + enumValue.Name.WireValue
		}
		t.writer.P("case \"", enumValue.Name.WireValue, "\":")
		t.writer.P("return ", enumName, ", nil")
	}
	t.writer.P("}")
	t.writer.P("var t ", t.typeName)
	t.writer.P(`return "", fmt.Errorf("%s is not a valid %T", s, t)`)
	t.writer.P("}")
	t.writer.P()

	// Generate a pointer method so that it's easier to use the enum as
	// an optional value.
	receiver := typeNameToReceiver(t.typeName)
	t.writer.P("func (", receiver, " ", t.typeName, ") Ptr() *", t.typeName, " {")
	t.writer.P("return &", receiver)
	t.writer.P("}")
	t.writer.P()

	return nil
}

func (t *typeVisitor) VisitObject(object *ir.ObjectTypeDeclaration) error {
	t.writer.P("type ", t.typeName, " struct {")
	_, literals := t.visitObjectProperties(object, true /* includeTags */, false /* includeOptionals */)

	// If the object has a literal, it needs custom [de]serialization logic,
	// and a getter method to access the field so that it's impossible for
	// the user to mutate it.
	//
	// Literals are ignored in visitObjectProperties, so we need to write them
	// here.
	for _, literal := range literals {
		t.writer.P(literal.Name.CamelCase.SafeName, " ", literalToGoType(literal.Value))
	}
	if t.includeRawJSON {
		t.writer.P()
		t.writer.P("_rawJSON json.RawMessage")
	}
	t.writer.P("}")
	t.writer.P()

	receiver := typeNameToReceiver(t.typeName)

	// Implement the getter methods.
	for _, literal := range literals {
		t.writer.P("func (", receiver, " *", t.typeName, ") ", literal.Name.PascalCase.UnsafeName, "()", literalToGoType(literal.Value), "{")
		t.writer.P("return ", receiver, ".", literal.Name.CamelCase.SafeName)
		t.writer.P("}")
		t.writer.P()
	}

	// Implement the json.Unmarshaler interface.
	if t.includeRawJSON || len(literals) > 0 {
		t.writer.P("func (", receiver, " *", t.typeName, ") UnmarshalJSON(data []byte) error {")
		t.writer.P("type unmarshaler ", t.typeName)
		t.writer.P("var value unmarshaler")
		t.writer.P("if err := json.Unmarshal(data, &value); err != nil {")
		t.writer.P("return err")
		t.writer.P("}")
		t.writer.P("*", receiver, " = ", t.typeName, "(value)")
		for _, literal := range literals {
			t.writer.P(receiver, ".", literal.Name.CamelCase.SafeName, " = ", literalToValue(literal.Value))
		}
		if t.includeRawJSON {
			t.writer.P(receiver, "._rawJSON = json.RawMessage(data)")
		}
		t.writer.P("return nil")
		t.writer.P("}")
		t.writer.P()
	}

	// Implement the json.Marshaler interface (if we have any literals).
	if len(literals) > 0 {
		t.writer.P("func (", receiver, " *", t.typeName, ") MarshalJSON() ([]byte, error) {")
		t.writer.P("type embed ", t.typeName)
		t.writer.P("var marshaler = struct{")
		t.writer.P("embed")
		for _, literal := range literals {
			t.writer.P(literal.Name.PascalCase.UnsafeName, " ", literalToGoType(literal.Value), " `json:\"", literal.Name.OriginalName, "\"`")
		}
		t.writer.P("}{")
		t.writer.P("embed: embed(*", receiver, "),")
		for _, literal := range literals {
			t.writer.P(literal.Name.PascalCase.UnsafeName, ": ", literalToValue(literal.Value), ",")
		}
		t.writer.P("}")
		t.writer.P("return json.Marshal(marshaler)")
		t.writer.P("}")
		t.writer.P()
	}

	// Implement fmt.Stringer.
	t.writer.P("func (", receiver, " *", t.typeName, ") String() string {")
	if t.includeRawJSON {
		t.writer.P("if len(", receiver, "._rawJSON) > 0 {")
		t.writer.P("if value, err := core.StringifyJSON(", receiver, "._rawJSON); err == nil {")
		t.writer.P("return value")
		t.writer.P("}")
		t.writer.P("}")
	}
	t.writer.P("if value, err := core.StringifyJSON(", receiver, "); err == nil {")
	t.writer.P("return value")
	t.writer.P("}")
	t.writer.P(`return fmt.Sprintf("%#v", `, receiver, ")")
	t.writer.P("}")
	t.writer.P()

	return nil
}

func (t *typeVisitor) VisitUnion(union *ir.UnionTypeDeclaration) error {
	// Write the union type definition.
	discriminantName := union.Discriminant.Name.PascalCase.UnsafeName
	t.writer.P("type ", t.typeName, " struct {")
	t.writer.P(discriminantName, " string")
	var literals []*literal
	for _, extend := range union.Extends {
		_, extendedLiterals := t.visitObjectProperties(t.writer.types[extend.TypeId].Shape.Object, false /* includeTags */, false /* includeOptionals */)
		literals = append(literals, extendedLiterals...)
	}
	for _, property := range union.BaseProperties {
		if property.ValueType.Container != nil && property.ValueType.Container.Literal != nil {
			literals = append(literals, &literal{Name: property.Name.Name, Value: property.ValueType.Container.Literal})
			continue
		}
		t.writer.P(property.Name.Name.PascalCase.UnsafeName, " ", typeReferenceToGoType(property.ValueType, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath, false))
	}
	// We handle the union's literals separate from the extended and base
	// literals because we only want to set them if they were actually
	// specified by the user.
	var unionLiterals []*literal
	for _, unionType := range union.Types {
		typeName := singleUnionTypePropertiesToGoType(unionType.Shape, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath)
		if typeName == "" {
			// If the union has no properties, there's nothing for us to do.
			continue
		}
		t.writer.WriteDocs(unionType.Docs)
		if unionType.Shape.PropertiesType == "singleProperty" {
			if unionType.Shape.SingleProperty.Type.Container != nil && unionType.Shape.SingleProperty.Type.Container.Literal != nil {
				// Literal fields must be un-exported.
				t.writer.P(unionType.DiscriminantValue.Name.CamelCase.SafeName, " ", typeName)
				unionLiterals = append(
					unionLiterals,
					&literal{
						Name:  unionType.DiscriminantValue.Name,
						Value: unionType.Shape.SingleProperty.Type.Container.Literal,
					},
				)
				continue
			}
		}
		t.writer.P(unionType.DiscriminantValue.Name.PascalCase.UnsafeName, " ", typeName)
	}
	for _, literal := range literals {
		t.writer.P(literal.Name.CamelCase.SafeName, " ", literalToGoType(literal.Value))
	}
	t.writer.P("}")
	t.writer.P()

	// Implement the constructors.
	for _, unionType := range union.Types {
		fieldName := unionType.DiscriminantValue.Name.PascalCase.UnsafeName
		if unionType.Shape.SingleProperty != nil && unionType.Shape.SingleProperty.Type.Container != nil && unionType.Shape.SingleProperty.Type.Container.Literal != nil {
			// The constructor for a literal shouldn't take any arguments.
			literal := unionType.Shape.SingleProperty.Type.Container.Literal
			fieldName = unionType.DiscriminantValue.Name.CamelCase.SafeName
			t.writer.P("func New", t.typeName, "With", unionType.DiscriminantValue.Name.PascalCase.UnsafeName, "() *", t.typeName, "{")
			t.writer.P("return &", t.typeName, "{", discriminantName, ": \"", unionType.DiscriminantValue.Name.OriginalName, "\", ", fieldName, ": ", literalToValue(literal), "}")
		} else {
			t.writer.P("func New", t.typeName, "From", unionType.DiscriminantValue.Name.PascalCase.UnsafeName, "(value ", singleUnionTypePropertiesToGoType(unionType.Shape, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath), ") *", t.typeName, "{")
			t.writer.P("return &", t.typeName, "{", discriminantName, ": \"", unionType.DiscriminantValue.Name.OriginalName, "\", ", fieldName, ": value}")
		}
		t.writer.P("}")
		t.writer.P()
	}

	receiver := typeNameToReceiver(t.typeName)

	// Implement the getter methods.
	for _, literal := range append(literals, unionLiterals...) {
		t.writer.P("func (", receiver, " *", t.typeName, ") ", literal.Name.PascalCase.UnsafeName, "()", literalToGoType(literal.Value), "{")
		t.writer.P("return ", receiver, ".", literal.Name.CamelCase.SafeName)
		t.writer.P("}")
		t.writer.P()
	}

	// Implement the json.Unmarshaler interface.
	t.writer.P("func (", receiver, " *", t.typeName, ") UnmarshalJSON(data []byte) error {")
	t.writer.P("var unmarshaler struct {")
	t.writer.P(discriminantName, " string `json:\"", union.Discriminant.WireValue, "\"`")
	var propertyNames []string
	for _, extend := range union.Extends {
		extendedProperties, _ := t.visitObjectProperties(t.writer.types[extend.TypeId].Shape.Object, true /* includeTags */, false /* includeOptionals */)
		propertyNames = append(propertyNames, extendedProperties...)
	}
	for _, property := range union.BaseProperties {
		if property.ValueType.Container != nil && property.ValueType.Container.Literal != nil {
			continue
		}
		propertyNames = append(propertyNames, property.Name.Name.PascalCase.UnsafeName)
		t.writer.P(property.Name.Name.PascalCase.UnsafeName, " ", typeReferenceToGoType(property.ValueType, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath, false), jsonTagForType(property.Name.WireValue, property.ValueType, t.writer.types))
	}
	t.writer.P("}")
	t.writer.P("if err := json.Unmarshal(data, &unmarshaler); err != nil {")
	t.writer.P("return err")
	t.writer.P("}")

	// Set the union's type on the exported type, plus all of the other
	// extended and/or base properties.
	t.writer.P(receiver, ".", discriminantName, " = unmarshaler.", discriminantName)
	for _, propertyName := range propertyNames {
		t.writer.P(receiver, ".", propertyName, " = unmarshaler.", propertyName)
	}
	for _, literal := range literals {
		t.writer.P(receiver, ".", literal.Name.CamelCase.SafeName, " = ", literalToValue(literal.Value))
	}

	// Generate the switch to unmarshal the appropriate type.
	t.writer.P("switch unmarshaler.", discriminantName, " {")
	for _, unionType := range union.Types {
		t.writer.P("case \"", unionType.DiscriminantValue.Name.OriginalName, "\":")
		if unionType.Shape.PropertiesType == "singleProperty" {
			if unionType.Shape.SingleProperty.Type.Container != nil && unionType.Shape.SingleProperty.Type.Container.Literal != nil {
				// We have a literal, so we need to set its value explicitly.
				literal := unionType.Shape.SingleProperty.Type.Container.Literal
				t.writer.P(receiver, ".", unionType.DiscriminantValue.Name.CamelCase.SafeName, " = ", literalToValue(literal))
				continue
			}
			// If the union is a single property, we need a separate unmarshaler.
			// We can't use the top-level unmarshaler because of potential property
			// serde name conflicts, e.g.
			//
			//  type unmarshaler struct {
			//    String string `json:"value"`
			//    Boolean bool  `json:"value"`
			//  }
			t.writer.P("var valueUnmarshaler struct {")
			typeName := singleUnionTypePropertiesToGoType(unionType.Shape, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath)
			t.writer.P(unionType.DiscriminantValue.Name.PascalCase.UnsafeName, " ", typeName, jsonTagForType(unionType.Shape.SingleProperty.Name.WireValue, unionType.Shape.SingleProperty.Type, t.writer.types))
			t.writer.P("}")
			t.writer.P("if err := json.Unmarshal(data, &valueUnmarshaler); err != nil {")
			t.writer.P("return err")
			t.writer.P("}")
			t.writer.P(receiver, ".", unionType.DiscriminantValue.Name.PascalCase.UnsafeName, " = valueUnmarshaler.", unionType.DiscriminantValue.Name.PascalCase.UnsafeName)
			continue
		}
		t.writer.P(singleUnionTypePropertiesToInitializer(unionType.Shape, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath, unionType.DiscriminantValue.Name.PascalCase.UnsafeName, receiver))
		t.writer.P("if err := json.Unmarshal(data, &value); err != nil {")
		t.writer.P("return err")
		t.writer.P("}")
		t.writer.P(receiver, ".", unionType.DiscriminantValue.Name.PascalCase.UnsafeName, " = value")
	}
	t.writer.P("}")
	t.writer.P("return nil")
	t.writer.P("}")
	t.writer.P()

	// Implement the json.Marshaler interface.
	t.writer.P("func (", receiver, " ", t.typeName, ") MarshalJSON() ([]byte, error) {")
	t.writer.P("switch ", receiver, ".", discriminantName, " {")
	for i, unionType := range union.Types {
		if i == 0 {
			// Implement the default case first.
			t.writer.P("default:")
			t.writer.P("return nil, fmt.Errorf(\"invalid type %s in %T\", ", receiver, ".", discriminantName, ", ", receiver, ")")
		}
		t.writer.P("case \"", unionType.DiscriminantValue.Name.OriginalName, "\":")
		t.writer.P("var marshaler = struct {")
		t.writer.P(discriminantName, " string `json:\"", union.Discriminant.WireValue, "\"`")
		// Include all of the extended and base properties.
		for _, extend := range union.Extends {
			_, _ = t.visitObjectProperties(t.writer.types[extend.TypeId].Shape.Object, true /* includeTags */, false /* includeOptionals */)
		}
		for _, property := range union.BaseProperties {
			if property.ValueType.Container != nil && property.ValueType.Container.Literal != nil {
				continue
			}
			t.writer.P(property.Name.Name.PascalCase.UnsafeName, " ", typeReferenceToGoType(property.ValueType, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath, false), jsonTagForType(property.Name.WireValue, property.ValueType, t.writer.types))
		}
		for _, literal := range literals {
			t.writer.P(literal.Name.PascalCase.UnsafeName, " ", literalToGoType(literal.Value), " `json:\"", literal.Name.OriginalName, "\"`")
		}
		typeName := singleUnionTypePropertiesToGoType(unionType.Shape, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath)
		switch unionType.Shape.PropertiesType {
		case "singleProperty":
			t.writer.P(unionType.DiscriminantValue.Name.PascalCase.UnsafeName, " ", typeName, jsonTagForType(unionType.Shape.SingleProperty.Name.WireValue, unionType.Shape.SingleProperty.Type, t.writer.types))
		case "samePropertiesAsObject":
			t.writer.P(typeName)
		case "noProperties":
			// For no properties, we always include the omitempty tag.
			t.writer.P(unionType.DiscriminantValue.Name.PascalCase.UnsafeName, " ", typeName, " `json:\"", unionType.DiscriminantValue.WireValue, ",omitempty\"`")
		}
		// Set all of the values in the marshaler.
		t.writer.P("}{")
		t.writer.P(discriminantName, ": ", receiver, ".", discriminantName, ",")
		for _, propertyName := range propertyNames {
			t.writer.P(propertyName, ": ", receiver, ".", propertyName, ",")
		}
		for _, literal := range literals {
			t.writer.P(literal.Name.PascalCase.UnsafeName, ": ", literalToValue(literal.Value), ",")
		}
		if unionType.Shape.SingleProperty != nil && unionType.Shape.SingleProperty.Type.Container != nil && unionType.Shape.SingleProperty.Type.Container.Literal != nil {
			literal := unionType.Shape.SingleProperty.Type.Container.Literal
			t.writer.P(unionType.DiscriminantValue.Name.PascalCase.UnsafeName, ": ", literalToValue(literal), ",")
		} else {
			marshalerFieldName := unionType.DiscriminantValue.Name.PascalCase.UnsafeName
			if unionType.Shape.PropertiesType == "samePropertiesAsObject" {
				// If the object is embedded, the field name is equivalent to
				// the object's name, without any leading pointers.
				marshalerFieldName = typeNameToFieldName(typeName)
			}
			t.writer.P(marshalerFieldName, ": ", receiver, ".", unionType.DiscriminantValue.Name.PascalCase.UnsafeName, ",")
		}
		t.writer.P("}")
		t.writer.P("return json.Marshal(marshaler)")
	}
	t.writer.P("}")
	t.writer.P("}")
	t.writer.P()

	// Generate the Visitor interface.
	t.writer.P("type ", t.typeName, "Visitor interface {")
	for _, unionType := range union.Types {
		t.writer.P("Visit", unionType.DiscriminantValue.Name.PascalCase.UnsafeName, "(", singleUnionTypePropertiesToGoType(unionType.Shape, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath), ") error")
	}
	t.writer.P("}")
	t.writer.P()

	// Generate the Accept method.
	t.writer.P("func (", receiver, " *", t.typeName, ") Accept(visitor ", t.typeName, "Visitor) error {")
	t.writer.P("switch ", receiver, ".", discriminantName, "{")
	for i, unionType := range union.Types {
		if i == 0 {
			// Implement the default case first.
			t.writer.P("default:")
			t.writer.P("return fmt.Errorf(\"invalid type %s in %T\", ", receiver, ".", discriminantName, ", ", receiver, ")")
		}
		t.writer.P("case \"", unionType.DiscriminantValue.Name.OriginalName, "\":")
		fieldName := unionType.DiscriminantValue.Name.PascalCase.UnsafeName
		if unionType.Shape.SingleProperty != nil && unionType.Shape.SingleProperty.Type.Container != nil && unionType.Shape.SingleProperty.Type.Container.Literal != nil {
			fieldName = unionType.DiscriminantValue.Name.CamelCase.SafeName
		}
		t.writer.P("return visitor.Visit", unionType.DiscriminantValue.Name.PascalCase.UnsafeName, "(", receiver, ".", fieldName, ")")
	}
	t.writer.P("}")
	t.writer.P("}")
	t.writer.P()

	return nil
}

func (t *typeVisitor) VisitUndiscriminatedUnion(union *ir.UndiscriminatedUnionTypeDeclaration) error {
	// member represents a single undiscriminated union member.
	//
	// We define a separate struct here so that we can collect
	// all of the relevant information upfront, and traverse it
	// multiple times seamlessly (and in the order they're specified).
	type member struct {
		typeName  string
		caseName  string
		field     string
		variable  string
		value     string
		docs      *string
		literal   string
		isLiteral bool
	}
	var members []*member
	var hasLiteral bool
	for _, unionMember := range union.Members {
		field := typeReferenceToUndiscriminatedUnionField(unionMember.Type, t.writer.types)
		var typeName string
		if unionMember.Type.Named != nil {
			typeName = unionMember.Type.Named.TypeId
		}
		var literal string
		isLiteral := unionMember.Type.Container != nil && unionMember.Type.Container.Literal != nil
		if isLiteral {
			literal = literalToValue(unionMember.Type.Container.Literal)
		}
		hasLiteral = hasLiteral || isLiteral
		members = append(
			members,
			&member{
				typeName:  typeName,
				field:     field,
				variable:  fmt.Sprintf("value%s", strings.Title(field)),
				caseName:  firstLetterToLower(field),
				value:     typeReferenceToGoType(unionMember.Type, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath, false),
				docs:      unionMember.Docs,
				literal:   literal,
				isLiteral: isLiteral,
			},
		)
	}

	// Write the union type definition.
	t.writer.P("type ", t.typeName, " struct {")
	t.writer.P("typeName string")
	for _, member := range members {
		t.writer.WriteDocs(member.docs)
		t.writer.P(member.field, " ", member.value)
	}
	t.writer.P("}")
	t.writer.P()

	// Implement the constructors.
	for _, member := range members {
		if member.isLiteral {
			t.writer.P("func New", t.typeName, "With", strings.Title(member.field), "() *", t.typeName, "{")
			t.writer.P("return &", t.typeName, "{typeName: \"", member.caseName, "\", ", member.field, ": ", member.literal, "}")
		} else {
			t.writer.P("func New", t.typeName, "From", member.field, "(value ", member.value, ") *", t.typeName, "{")
			t.writer.P("return &", t.typeName, "{typeName: \"", member.caseName, "\", ", member.field, ": value}")
		}
		t.writer.P("}")
		t.writer.P()
	}

	receiver := typeNameToReceiver(t.typeName)

	// Write getters for literal values, if any.
	for _, member := range members {
		if !member.isLiteral {
			continue
		}
		t.writer.P("func (", receiver, " *", t.typeName, ") ", strings.Title(member.field), "() ", member.value, "{")
		t.writer.P("return ", receiver, ".", member.field)
		t.writer.P("}")
		t.writer.P()
	}

	// Implement the json.Unmarshaler interface.
	t.writer.P("func (", receiver, " *", t.typeName, ") UnmarshalJSON(data []byte) error {")
	for _, member := range members {
		value := member.value
		format := "var " + member.variable + " %s"
		if member.typeName != "" && isPointer(t.writer.types[member.typeName]) {
			format = member.variable + " := new(%s)"
			value = strings.TrimLeft(value, "*")
		}
		t.writer.P(fmt.Sprintf(format, value))
		t.writer.P("if err := json.Unmarshal(data, &", member.variable, "); err == nil {")
		if member.isLiteral {
			// If the undiscriminated union specifies a literal, it will only
			// succeed if the literal matches exactly.
			t.writer.P("if ", member.variable, "== ", member.literal, " {")
			t.writer.P(receiver, ".typeName = \"", member.caseName, "\"")
			t.writer.P(receiver, ".", member.field, " = ", member.variable)
			t.writer.P("return nil")
			t.writer.P("}")
			t.writer.P("}")
			continue
		}
		t.writer.P(receiver, ".typeName = \"", member.caseName, "\"")
		t.writer.P(receiver, ".", member.field, " = ", member.variable)
		t.writer.P("return nil")
		t.writer.P("}")
	}
	t.writer.P(`return fmt.Errorf("%s cannot be deserialized as a %T", data, `, receiver, ")")
	t.writer.P("}")
	t.writer.P()

	t.writer.P("func (", receiver, " ", t.typeName, ") MarshalJSON() ([]byte, error) {")
	t.writer.P("switch ", receiver, ".typeName {")
	for i, member := range members {
		if i == 0 {
			// Implement the default case first.
			t.writer.P("default:")
			t.writer.P("return nil, fmt.Errorf(\"invalid type %s in %T\", ", receiver, ".typeName, ", receiver, ")")
		}
		t.writer.P("case \"", member.caseName, "\":")
		if member.isLiteral {
			// If we have a literal, we need to marshal it directly.
			t.writer.P("return json.Marshal(", member.literal, ")")
			continue
		}
		t.writer.P("return json.Marshal(", receiver, ".", member.field, ")")
	}
	t.writer.P("}")
	t.writer.P("}")
	t.writer.P()

	// Generate the Visitor interface.
	t.writer.P("type ", t.typeName, "Visitor interface {")
	for _, member := range members {
		t.writer.P("Visit", strings.Title(member.field), "(", member.value, ") error")
	}
	t.writer.P("}")
	t.writer.P()

	// Generate the Accept method.
	t.writer.P("func (", receiver, " *", t.typeName, ") Accept(visitor ", t.typeName, "Visitor) error {")
	t.writer.P("switch ", receiver, ".typeName {")
	for i, member := range members {
		if i == 0 {
			// Implement the default case first.
			t.writer.P("default:")
			t.writer.P("return fmt.Errorf(\"invalid type %s in %T\", ", receiver, ".typeName, ", receiver, ")")
		}
		t.writer.P("case \"", member.caseName, "\":")
		t.writer.P("return visitor.Visit", strings.Title(member.field), "(", receiver, ".", member.field, ")")
	}
	t.writer.P("}")
	t.writer.P("}")
	t.writer.P()

	return nil
}

// undiscriminatedUnionTypeReferenceVisitor retrieves the string representation of type references
// (e.g. containers, primitives, etc), but specifically for undiscriminated union generation.
type undiscriminatedUnionTypeReferenceVisitor struct {
	value string
	types map[ir.TypeId]*ir.TypeDeclaration
}

// Compile-time assertion.
var _ ir.TypeReferenceVisitor = (*undiscriminatedUnionTypeReferenceVisitor)(nil)

func (u *undiscriminatedUnionTypeReferenceVisitor) VisitContainer(container *ir.ContainerType) error {
	u.value = containerToUndiscriminatedUnionField(container, u.types)
	return nil
}

func (u *undiscriminatedUnionTypeReferenceVisitor) VisitNamed(named *ir.DeclaredTypeName) error {
	u.value = named.Name.PascalCase.UnsafeName
	return nil
}

func (u *undiscriminatedUnionTypeReferenceVisitor) VisitPrimitive(primitive ir.PrimitiveType) error {
	u.value = primitiveToUndiscriminatedUnionField(primitive)
	return nil
}

func (u *undiscriminatedUnionTypeReferenceVisitor) VisitUnknown(unknown any) error {
	u.value = "Unknown"
	return nil
}

// undiscriminatedUnionContainerTypeVisitor retrieves the string representation of container types
// (e.g. lists, maps, etc).
type undiscriminatedUnionContainerTypeVisitor struct {
	value string
	types map[ir.TypeId]*ir.TypeDeclaration
}

// Compile-time assertion.
var _ ir.ContainerTypeVisitor = (*undiscriminatedUnionContainerTypeVisitor)(nil)

func (u *undiscriminatedUnionContainerTypeVisitor) VisitList(list *ir.TypeReference) error {
	u.value = fmt.Sprintf("%sList", typeReferenceToUndiscriminatedUnionField(list, u.types))
	return nil
}

func (u *undiscriminatedUnionContainerTypeVisitor) VisitMap(mapType *ir.MapType) error {
	u.value = fmt.Sprintf(
		"%s%sMap",
		typeReferenceToUndiscriminatedUnionField(mapType.KeyType, u.types),
		typeReferenceToUndiscriminatedUnionField(mapType.ValueType, u.types),
	)
	return nil
}

func (u *undiscriminatedUnionContainerTypeVisitor) VisitOptional(optional *ir.TypeReference) error {
	u.value = fmt.Sprintf("%sOptional", typeReferenceToUndiscriminatedUnionField(optional, u.types))
	return nil
}

func (u *undiscriminatedUnionContainerTypeVisitor) VisitSet(set *ir.TypeReference) error {
	u.value = fmt.Sprintf("%sSet", typeReferenceToUndiscriminatedUnionField(set, u.types))
	return nil
}

func (u *undiscriminatedUnionContainerTypeVisitor) VisitLiteral(literal *ir.Literal) error {
	u.value = fmt.Sprintf("%sLiteral", literalToUndiscriminatedUnionField(literal))
	return nil
}

// literal contains the information required to generate code for literal properties.
type literal struct {
	Name  *ir.Name
	Value *ir.Literal
}

// visitObjectProperties writes all of this object's properties, and recursively calls itself with
// the object's extended properties (if any). The 'includeTags' parameter controls whether or not
// to generate JSON struct tags, which is only relevant for object types (not unions).
//
// A slice of all the transitive property names, as well as a sentinel value that signals whether
// any of the properties are a literal value, are returned.
func (t *typeVisitor) visitObjectProperties(
	object *ir.ObjectTypeDeclaration,
	includeTags bool,
	includeOptionals bool,
) ([]string, []*literal) {
	var names []string
	var literals []*literal
	for _, extend := range object.Extends {
		// You can only extend other objects.
		extendedNames, extendedLiterals := t.visitObjectProperties(t.writer.types[extend.TypeId].Shape.Object, includeTags, includeOptionals)
		names = append(names, extendedNames...)
		literals = append(literals, extendedLiterals...)
	}
	for _, property := range object.Properties {
		t.writer.WriteDocs(property.Docs)
		if property.ValueType.Container != nil && property.ValueType.Container.Literal != nil {
			literals = append(literals, &literal{Name: property.Name.Name, Value: property.ValueType.Container.Literal})
			continue
		}
		names = append(names, property.Name.Name.PascalCase.UnsafeName)
		goType := typeReferenceToGoType(property.ValueType, t.writer.types, t.writer.scope, t.baseImportPath, t.importPath, includeOptionals)
		if includeTags {
			t.writer.P(property.Name.Name.PascalCase.UnsafeName, " ", goType, jsonTagForType(property.Name.WireValue, property.ValueType, t.writer.types))
			continue
		}
		t.writer.P(property.Name.Name.PascalCase.UnsafeName, " ", goType)
	}
	return names, literals
}

// typeReferenceVisitor retrieves the string representation of type references
// (e.g. containers, primitives, etc).
type typeReferenceVisitor struct {
	value            string
	baseImportPath   string
	importPath       string
	scope            *gospec.Scope
	types            map[ir.TypeId]*ir.TypeDeclaration
	includeOptionals bool
}

// Compile-time assertion.
var _ ir.TypeReferenceVisitor = (*typeReferenceVisitor)(nil)

func (t *typeReferenceVisitor) VisitContainer(container *ir.ContainerType) error {
	t.value = containerTypeToGoType(container, t.types, t.scope, t.baseImportPath, t.importPath, t.includeOptionals)
	return nil
}

func (t *typeReferenceVisitor) VisitNamed(named *ir.DeclaredTypeName) error {
	format := "%s"
	if isPointer(t.types[named.TypeId]) {
		format = "*%s"
	}
	name := named.Name.PascalCase.UnsafeName
	if importPath := fernFilepathToImportPath(t.baseImportPath, named.FernFilepath); importPath != t.importPath {
		name = t.scope.AddImport(importPath) + "." + name
	}
	t.value = fmt.Sprintf(format, name)
	return nil
}

func (t *typeReferenceVisitor) VisitPrimitive(primitive ir.PrimitiveType) error {
	t.value = primitiveToGoType(primitive)
	return nil
}

func (t *typeReferenceVisitor) VisitUnknown(unknown any) error {
	t.value = unknownToGoType(unknown)
	return nil
}

// containerTypeVisitor retrieves the string representation of container types
// (e.g. lists, maps, etc).
type containerTypeVisitor struct {
	value            string
	baseImportPath   string
	importPath       string
	scope            *gospec.Scope
	types            map[ir.TypeId]*ir.TypeDeclaration
	includeOptionals bool
}

// Compile-time assertion.
var _ ir.ContainerTypeVisitor = (*containerTypeVisitor)(nil)

func (c *containerTypeVisitor) VisitList(list *ir.TypeReference) error {
	c.value = fmt.Sprintf("[]%s", typeReferenceToGoType(list, c.types, c.scope, c.baseImportPath, c.importPath, false))
	return nil
}

func (c *containerTypeVisitor) VisitMap(mapType *ir.MapType) error {
	c.value = fmt.Sprintf("map[%s]%s", typeReferenceToGoType(mapType.KeyType, c.types, c.scope, c.baseImportPath, c.importPath, false), typeReferenceToGoType(mapType.ValueType, c.types, c.scope, c.baseImportPath, c.importPath, false))
	return nil
}

func (c *containerTypeVisitor) VisitOptional(optional *ir.TypeReference) error {
	// Trim all of the preceding pointers from the underlying type so that we don't
	// unnecessarily generate double pointers for objects and unions (e.g. '**Foo)').
	//
	// We also don't want to specify pointers for any container types because those
	// values are already nil-able.
	value := strings.TrimLeft(typeReferenceToGoType(optional, c.types, c.scope, c.baseImportPath, c.importPath, c.includeOptionals), "*")
	if c.includeOptionals {
		c.value = fmt.Sprintf("*core.Optional[%s]", value)
		return nil
	}
	if optional.Unknown != nil || (optional.Container != nil && optional.Container.Literal == nil) {
		c.value = value
		return nil
	}
	c.value = fmt.Sprintf("*%s", value)
	return nil
}

func (c *containerTypeVisitor) VisitSet(set *ir.TypeReference) error {
	c.value = fmt.Sprintf("[]%s", typeReferenceToGoType(set, c.types, c.scope, c.baseImportPath, c.importPath, false))
	return nil
}

func (c *containerTypeVisitor) VisitLiteral(literal *ir.Literal) error {
	c.value = literalToGoType(literal)
	return nil
}

// singleUnionTypePropertiesVisitor retrieves the string representation of
// single union type properties.
type singleUnionTypePropertiesVisitor struct {
	value          string
	baseImportPath string
	importPath     string
	scope          *gospec.Scope
	types          map[ir.TypeId]*ir.TypeDeclaration
}

// Compile-time assertion.
var _ ir.SingleUnionTypePropertiesVisitor = (*singleUnionTypePropertiesVisitor)(nil)

func (c *singleUnionTypePropertiesVisitor) VisitSamePropertiesAsObject(named *ir.DeclaredTypeName) error {
	format := "%s"
	if isPointer(c.types[named.TypeId]) {
		format = "*%s"
	}
	name := named.Name.PascalCase.UnsafeName
	if importPath := fernFilepathToImportPath(c.baseImportPath, named.FernFilepath); importPath != c.importPath {
		name = c.scope.AddImport(importPath) + "." + name
	}
	c.value = fmt.Sprintf(format, name)
	return nil
}

func (c *singleUnionTypePropertiesVisitor) VisitSingleProperty(property *ir.SingleUnionTypeProperty) error {
	c.value = typeReferenceToGoType(property.Type, c.types, c.scope, c.baseImportPath, c.importPath, false)
	return nil
}

func (c *singleUnionTypePropertiesVisitor) VisitNoProperties(_ any) error {
	c.value = "interface{}"
	return nil
}

// singleUnionTypePropertiesInitializerVisitor retrieves the string representation of
// single union type property initializers.
//
// This visitor determines the string representation of the constructor used
// in the json.Unmarshaler implementation.
type singleUnionTypePropertiesInitializerVisitor struct {
	value            string
	discriminantName string
	receiver         string
	baseImportPath   string
	importPath       string
	scope            *gospec.Scope
	types            map[ir.TypeId]*ir.TypeDeclaration
}

// Compile-time assertion.
var _ ir.SingleUnionTypePropertiesVisitor = (*singleUnionTypePropertiesInitializerVisitor)(nil)

func (c *singleUnionTypePropertiesInitializerVisitor) VisitSamePropertiesAsObject(named *ir.DeclaredTypeName) error {
	format := "var value %s"
	if isPointer(c.types[named.TypeId]) {
		format = "value := new(%s)"
	}
	name := named.Name.PascalCase.UnsafeName
	if importPath := fernFilepathToImportPath(c.baseImportPath, named.FernFilepath); importPath != c.importPath {
		name = c.scope.AddImport(importPath) + "." + name
	}
	c.value = fmt.Sprintf(format, name)
	return nil
}

func (c *singleUnionTypePropertiesInitializerVisitor) VisitSingleProperty(_ *ir.SingleUnionTypeProperty) error {
	c.value = fmt.Sprintf("%s.%s = unmarshaler.%s", c.receiver, c.discriminantName, c.discriminantName)
	return nil
}

func (c *singleUnionTypePropertiesInitializerVisitor) VisitNoProperties(_ any) error {
	c.value = "value := make(map[string]interface{})"
	return nil
}

// literalValueVisitor retrieves the string representation of the literal value.
// Strings are the only supported literals for now.
type literalValueVisitor struct {
	value string
}

// Compile-time assertion.
var _ ir.LiteralVisitor = (*literalValueVisitor)(nil)

func (l *literalValueVisitor) VisitBoolean(value bool) error {
	l.value = strconv.FormatBool(value)
	return nil
}

func (l *literalValueVisitor) VisitString(value string) error {
	l.value = fmt.Sprintf("%q", value)
	return nil
}

// literalTypeVisitor retrieves the string representation of the literal's type.
// Strings are the only supported literals for now.
type literalTypeVisitor struct {
	value string
}

// Compile-time assertion.
var _ ir.LiteralVisitor = (*literalTypeVisitor)(nil)

func (l *literalTypeVisitor) VisitBoolean(_ bool) error {
	l.value = "bool"
	return nil
}

func (l *literalTypeVisitor) VisitString(_ string) error {
	l.value = "string"
	return nil
}

// typeReferenceToGoType maps the given type reference into its Go-equivalent.
func typeReferenceToGoType(
	typeReference *ir.TypeReference,
	types map[ir.TypeId]*ir.TypeDeclaration,
	scope *gospec.Scope,
	baseImportPath string,
	importPath string,
	includeOptionals bool,
) string {
	visitor := &typeReferenceVisitor{
		baseImportPath:   baseImportPath,
		importPath:       importPath,
		scope:            scope,
		types:            types,
		includeOptionals: includeOptionals,
	}
	_ = typeReference.Accept(visitor)
	return visitor.value
}

// containerTypeToGoType maps the given container type into its Go-equivalent.
func containerTypeToGoType(
	containerType *ir.ContainerType,
	types map[ir.TypeId]*ir.TypeDeclaration,
	scope *gospec.Scope,
	baseImportPath string,
	importPath string,
	includeOptionals bool,
) string {
	visitor := &containerTypeVisitor{
		baseImportPath:   baseImportPath,
		importPath:       importPath,
		scope:            scope,
		types:            types,
		includeOptionals: includeOptionals,
	}
	_ = containerType.Accept(visitor)
	return visitor.value
}

// singleUnionTypePropertiesToGoType maps the given container type into its Go-equivalent.
func singleUnionTypePropertiesToGoType(
	singleUnionTypeProperties *ir.SingleUnionTypeProperties,
	types map[ir.TypeId]*ir.TypeDeclaration,
	scope *gospec.Scope,
	baseImportPath string,
	importPath string,
) string {
	visitor := &singleUnionTypePropertiesVisitor{
		baseImportPath: baseImportPath,
		importPath:     importPath,
		scope:          scope,
		types:          types,
	}
	_ = singleUnionTypeProperties.Accept(visitor)
	return visitor.value
}

// singleUnionTypePropertiesToInitializer maps the given container type into its Go-equivalent initializer.
//
// Note this returns the string representation of the statement required to initialize
// the given property, e.g. 'value := new(Foo)'
func singleUnionTypePropertiesToInitializer(
	singleUnionTypeProperties *ir.SingleUnionTypeProperties,
	types map[ir.TypeId]*ir.TypeDeclaration,
	scope *gospec.Scope,
	baseImportPath string,
	importPath string,
	discriminantName string,
	receiver string,
) string {
	visitor := &singleUnionTypePropertiesInitializerVisitor{
		discriminantName: discriminantName,
		receiver:         receiver,
		baseImportPath:   baseImportPath,
		importPath:       importPath,
		scope:            scope,
		types:            types,
	}
	_ = singleUnionTypeProperties.Accept(visitor)
	return visitor.value
}

// typeReferenceToUndiscriminatedUnionField maps Fern's type references to the field name used in an
// undiscriminated union.
func typeReferenceToUndiscriminatedUnionField(typeReference *ir.TypeReference, types map[ir.TypeId]*ir.TypeDeclaration) string {
	visitor := &undiscriminatedUnionTypeReferenceVisitor{
		types: types,
	}
	_ = typeReference.Accept(visitor)
	return visitor.value
}

// containerToUndiscriminatedUnionField maps Fern's container types to the field name used in an
// undiscriminated union.
func containerToUndiscriminatedUnionField(container *ir.ContainerType, types map[ir.TypeId]*ir.TypeDeclaration) string {
	visitor := &undiscriminatedUnionContainerTypeVisitor{
		types: types,
	}
	_ = container.Accept(visitor)
	return visitor.value
}

// literalToGoType maps the given literal into its Go-equivalent.
func literalToGoType(literal *ir.Literal) string {
	visitor := new(literalTypeVisitor)
	_ = literal.Accept(visitor)
	return visitor.value
}

// literalToValue maps the given literal into its value.
func literalToValue(literal *ir.Literal) string {
	visitor := new(literalValueVisitor)
	_ = literal.Accept(visitor)
	return visitor.value
}

// fernFilepathToImportPath maps the given Fern filepath to its
// Go import path.
func fernFilepathToImportPath(baseImportPath string, fernFilepath *ir.FernFilepath) string {
	var packages []string
	for _, packageName := range fernFilepath.PackagePath {
		packages = append(packages, strings.ToLower(packageName.CamelCase.SafeName))
	}
	return packagePathToImportPath(baseImportPath, packages)
}

// packagePathToImportPath maps the given packagePath to its Go import path.
func packagePathToImportPath(baseImportPath string, packagePath []string) string {
	return path.Join(append([]string{baseImportPath}, packagePath...)...)
}

// isPointer returns true if the given type is a pointer type (e.g. objects and
// unions). Enums, primitives, and aliases of these types do not require pointers.
func isPointer(typeDeclaration *ir.TypeDeclaration) bool {
	switch typeDeclaration.Shape.Type {
	case "object", "union", "undiscriminatedUnion":
		return true
	case "alias", "enum", "primitive":
		return false
	}
	// Unreachable.
	return false
}

// typeNameToReceiver returns the receiver name for
// the given type name. This is just the lowercase
// equivalent of the first character.
func typeNameToReceiver(typeName string) string {
	if len(typeName) == 0 {
		return typeName
	}
	r := []rune(typeName)
	return string(unicode.ToLower(r[0]))
}

// firstLetterToLower returns the given string with its
// first letter lowercased.
func firstLetterToLower(s string) string {
	if len(s) == 0 {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToLower(r[0])
	return string(r)
}

// jsonTagForType returns the JSON tag for the given type. If the value type
// is a required primitive, then we don't include the omitempty tag so that
// the default values can be explicitly sent (e.g. "false" for bools).
func jsonTagForType(wireValue string, valueType *ir.TypeReference, types map[ir.TypeId]*ir.TypeDeclaration) string {
	if valueType != nil {
		primitive := valueType.Primitive
		if valueType.Named != nil {
			// If the type is an alias, we need to check if it's an alias to a primitive.
			if typeDeclaration := types[valueType.Named.TypeId]; typeDeclaration.Shape.Alias != nil {
				primitive = typeDeclaration.Shape.Alias.AliasOf.Primitive
			}
		}
		if primitive != "" {
			return fmt.Sprintf(" `json:%q`", wireValue)
		}
	}
	return fmt.Sprintf(" `json:\"%s,omitempty\"`", wireValue)
}

// unknownToGoType maps the given unknown into its Go-equivalent.
func unknownToGoType(_ any) string {
	return "interface{}"
}

// literalToUndiscriminatedUnionField maps Fern's literal types to the field name used in an
// undiscriminated union.
func literalToUndiscriminatedUnionField(literal *ir.Literal) string {
	switch literal.Type {
	case "string":
		return "string"
	default:
		return "unknown"
	}
}

// primitiveToGoType maps Fern's primitive types to their Go-equivalent.
func primitiveToGoType(primitive ir.PrimitiveType) string {
	switch primitive {
	case ir.PrimitiveTypeInteger:
		return "int"
	case ir.PrimitiveTypeDouble:
		return "float64"
	case ir.PrimitiveTypeString:
		return "string"
	case ir.PrimitiveTypeBoolean:
		return "bool"
	case ir.PrimitiveTypeLong:
		return "int64"
	case ir.PrimitiveTypeDateTime:
		return "time.Time"
	case ir.PrimitiveTypeDate:
		return "time.Time"
	case ir.PrimitiveTypeUuid:
		return "uuid.UUID"
	case ir.PrimitiveTypeBase64:
		return "[]byte"
	default:
		return "interface{}"
	}
}

// primitiveToUndiscriminatedUnionField maps Fern's primitive types to the field name used in an
// undiscriminated union.
func primitiveToUndiscriminatedUnionField(primitive ir.PrimitiveType) string {
	switch primitive {
	case ir.PrimitiveTypeInteger:
		return "Integer"
	case ir.PrimitiveTypeDouble:
		return "Double"
	case ir.PrimitiveTypeString:
		return "String"
	case ir.PrimitiveTypeBoolean:
		return "Boolean"
	case ir.PrimitiveTypeLong:
		return "Long"
	case ir.PrimitiveTypeDateTime:
		return "DateTime"
	case ir.PrimitiveTypeDate:
		return "Date"
	case ir.PrimitiveTypeUuid:
		return "Uuid"
	case ir.PrimitiveTypeBase64:
		return "Base64"
	default:
		return "Any"
	}
}

// typeNameToFieldName maps the given type name (e.g. *foo.Bar) into its embedded field name
// equivalent (e.g. Bar).
func typeNameToFieldName(typeName string) string {
	if split := strings.Split(typeName, "."); len(split) > 1 {
		return split[len(split)-1]
	}
	return strings.TrimLeft(typeName, "*")
}

// defaultValueForTypeReference returns the default value associated with the given *ir.TypeReference.
// For named types and built-ins, this will just be nil, otherwise it will be the associated primitive
// value.
func defaultValueForTypeReference(typeReference *ir.TypeReference, types map[string]*ir.TypeDeclaration) string {
	if typeReference.Container != nil {
		if typeReference.Container.Literal != nil {
			return ""
		}
		return "nil"
	}
	if typeReference.Named != nil {
		return defaultValueForTypeDeclaration(types[typeReference.Named.TypeId], types)
	}
	if typeReference.Primitive != "" {
		return defaultValueForPrimitiveType(typeReference.Primitive)
	}
	return "nil"
}

func defaultValueForTypeDeclaration(typeDeclaration *ir.TypeDeclaration, types map[string]*ir.TypeDeclaration) string {
	if typeDeclaration.Shape.Alias != nil {
		return defaultValueForTypeReference(typeDeclaration.Shape.Alias.AliasOf, types)
	}
	if typeDeclaration.Shape.Enum != nil {
		return ""
	}
	return "nil"
}

func defaultValueForPrimitiveType(primitiveType ir.PrimitiveType) string {
	switch primitiveType {
	case ir.PrimitiveTypeInteger:
		return "0"
	case ir.PrimitiveTypeDouble:
		return "0"
	case ir.PrimitiveTypeString:
		return `""`
	case ir.PrimitiveTypeBoolean:
		return "false"
	case ir.PrimitiveTypeLong:
		return "0"
	case ir.PrimitiveTypeDateTime:
		return "time.Time{}"
	case ir.PrimitiveTypeDate:
		return "time.Time{}"
	case ir.PrimitiveTypeUuid:
		return "uuid.Nil"
	case ir.PrimitiveTypeBase64:
		return "nil"
	}
	return "nil"
}
