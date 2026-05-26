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

	tr, err := NewTranslator(dir)
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
	_, err := NewTranslator("/nonexistent/path/that/should/not/exist")
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

	tr, err := NewTranslator(dir)
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}
	if tr == nil {
		t.Fatal("NewTranslator() returned nil")
	}
}

func TestTranslator_Translate_KnownMsgId(t *testing.T) {
	dir := setupTestLangFiles(t)
	tr, err := NewTranslator(dir)
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
			got := tr.Translate(tt.lang, tt.msgId)
			if got != tt.expect {
				t.Errorf("Translate(%q, %q) = %q, want %q", tt.lang, tt.msgId, got, tt.expect)
			}
		})
	}
}

func TestTranslator_Translate_UnknownMsgId(t *testing.T) {
	dir := setupTestLangFiles(t)
	tr, err := NewTranslator(dir)
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}

	got := tr.Translate("zh-CN", "888888")
	if got != "未知错误" {
		t.Errorf("Translate unknown msgId = %q, want %q", got, "未知错误")
	}

	got = tr.Translate("en", "888888")
	if got != "unknown error" {
		t.Errorf("Translate unknown msgId (en) = %q, want %q", got, "unknown error")
	}
}

func TestTranslator_Translate_DefaultMsgId(t *testing.T) {
	dir := setupTestLangFiles(t)
	tr, err := NewTranslator(dir)
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}

	got := tr.Translate("zh-CN", defaultMsgId)
	if got != "未知错误" {
		t.Errorf("Translate defaultMsgId = %q, want %q", got, "未知错误")
	}
}

func TestSetDefaultTranslator_And_PackageLevelTranslate(t *testing.T) {
	dir := setupTestLangFiles(t)
	tr, err := NewTranslator(dir)
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}

	SetDefaultTranslator(tr)

	got := Translate("en", "100001")
	if got != "parameter error" {
		t.Errorf("Translate() = %q, want %q", got, "parameter error")
	}

	got = Translate("zh-CN", "200001")
	if got != "余额不足" {
		t.Errorf("Translate() = %q, want %q", got, "余额不足")
	}
}

func TestTranslator_Translate_NoDefaultMsg(t *testing.T) {
	dir := t.TempDir()
	content := []byte(`100001="hello"`)
	if err := os.WriteFile(filepath.Join(dir, "en.toml"), content, 0644); err != nil {
		t.Fatal(err)
	}

	tr, err := NewTranslator(dir)
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}

	got := tr.Translate("en", "999999")
	if got == "" {
		t.Error("Translate() returned empty string for missing default msg")
	}
}
