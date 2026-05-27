package errs

import (
	"os"
	"path/filepath"
	"testing"
)

func setupTestLangFiles(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	zhContent := []byte(`999999="未知错误"
100001="参数错误"
200001="余额不足"
`)
	enContent := []byte(`999999="unknown error"
100001="parameter error"
200001="insufficient balance"
`)

	if err := os.WriteFile(filepath.Join(dir, "zh-CN.toml"), zhContent, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "en.toml"), enContent, 0644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestNewTranslator(t *testing.T) {
	dir := setupTestLangFiles(t)

	tr, err := NewTranslatorFormFile(dir)
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}
	if tr == nil {
		t.Fatal("NewTranslator() returned nil")
	}
	if tr.Bundle == nil {
		t.Fatal("Translator.Bundle is nil")
	}
}

func TestNewTranslator_InvalidPath(t *testing.T) {
	_, err := NewTranslatorFormFile("/nonexistent/path/that/should/not/exist")
	if err == nil {
		t.Fatal("NewTranslator() expected error for invalid path, got nil")
	}
}

func TestNewTranslator_SkipsNonToml(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "en.toml"), []byte(`100001="ok"`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte(`should be ignored`), 0644); err != nil {
		t.Fatal(err)
	}

	tr, err := NewTranslatorFormFile(dir)
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}
	if tr == nil {
		t.Fatal("NewTranslator() returned nil")
	}
}

func TestTranslator_Translate_KnownMsgId(t *testing.T) {
	dir := setupTestLangFiles(t)
	tr, err := NewTranslatorFormFile(dir)
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}

	tests := []struct {
		name   string
		lang   string
		msgId  string
		expect string
	}{
		{"zh param error", "zh-CN", "100001", "参数错误"},
		{"zh balance", "zh-CN", "200001", "余额不足"},
		{"en param error", "en", "100001", "parameter error"},
		{"en balance", "en", "200001", "insufficient balance"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := tr.Translate(tt.lang, tt.msgId)
			if got != tt.expect {
				t.Errorf("Translate(%q, %q) = %q, want %q", tt.lang, tt.msgId, got, tt.expect)
			}
		})
	}
}
