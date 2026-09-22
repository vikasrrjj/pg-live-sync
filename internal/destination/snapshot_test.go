package destination

import "testing"

func TestTextPointer(t *testing.T) {
	if got := textPointer(nil); got != nil {
		t.Fatalf("expected nil for NULL input, got %v", got)
	}

	text := "value"
	if got := textPointer(text); got == nil || *got != text {
		t.Fatalf("textPointer(string) = %v, want &%q", got, text)
	}

	in := &text
	if got := textPointer(in); got != in {
		t.Fatalf("textPointer(*string) should pass the pointer through, got %v", got)
	}

	if got := textPointer(42); got == nil || *got != "42" {
		t.Fatalf("textPointer(int) = %v, want &\"42\"", got)
	}
}
