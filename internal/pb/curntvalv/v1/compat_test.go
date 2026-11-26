package pb

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/Vympel87/curntvalv/internal/compat"
)

// TestAPICompat lists every service, message field and enum value of
// curntvalv.v1 against compat/api.txt (design D34): the API between nodes
// and clients, and between nodes of different versions.
func TestAPICompat(t *testing.T) {
	var lines []string
	protoregistry.GlobalFiles.RangeFilesByPackage("curntvalv.v1", func(fd protoreflect.FileDescriptor) bool {
		for i := range fd.Services().Len() {
			s := fd.Services().Get(i)
			for j := range s.Methods().Len() {
				m := s.Methods().Get(j)
				lines = append(lines, fmt.Sprintf("rpc %s.%s(%s%s) returns (%s%s)", s.Name(), m.Name(),
					stream(m.IsStreamingClient()), m.Input().FullName(), stream(m.IsStreamingServer()), m.Output().FullName()))
			}
		}
		lines = append(lines, messages(fd.Messages())...)
		lines = append(lines, enums(fd.Enums())...)
		return true
	})
	if len(lines) < 100 {
		t.Fatalf("only %d lines: is the package registered?", len(lines))
	}
	compat.Check(t, "api.txt", lines, compat.Updating())
}

func stream(on bool) string {
	if on {
		return "stream "
	}
	return ""
}

func messages(ms protoreflect.MessageDescriptors) []string {
	var out []string
	for i := range ms.Len() {
		m := ms.Get(i)
		if m.IsMapEntry() {
			continue
		}
		out = append(out, fmt.Sprintf("message %s", m.FullName()))
		for j := range m.Fields().Len() {
			f := m.Fields().Get(j)
			line := fmt.Sprintf("field %s %d %s %s", m.FullName(), f.Number(), f.Name(), fieldType(f))
			if o := f.ContainingOneof(); o != nil && !o.IsSynthetic() {
				line += " oneof " + string(o.Name())
			}
			out = append(out, line)
		}
		out = append(out, messages(m.Messages())...)
		out = append(out, enums(m.Enums())...)
	}
	return out
}

func fieldType(f protoreflect.FieldDescriptor) string {
	if f.IsMap() {
		return fmt.Sprintf("map<%s, %s>", fieldType(f.MapKey()), fieldType(f.MapValue()))
	}
	var typ string
	switch f.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		typ = string(f.Message().FullName())
	case protoreflect.EnumKind:
		typ = string(f.Enum().FullName())
	default:
		typ = f.Kind().String()
	}
	if f.Cardinality() == protoreflect.Repeated {
		typ = "repeated " + typ
	}
	if f.HasPresence() && f.ContainingOneof() != nil && f.ContainingOneof().IsSynthetic() {
		typ = "optional " + typ
	}
	return strings.TrimSpace(typ)
}

func enums(es protoreflect.EnumDescriptors) []string {
	var out []string
	for i := range es.Len() {
		e := es.Get(i)
		for j := range e.Values().Len() {
			v := e.Values().Get(j)
			out = append(out, fmt.Sprintf("enum %s %d %s", e.FullName(), v.Number(), v.Name()))
		}
	}
	return out
}
