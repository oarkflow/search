package main

import (
	"fmt"
	"log"
	"math"
	"os"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/oarkflow/search/v1"
)

type indexedMsg struct{ current, total int }
type indexDoneMsg struct{ index *v1.InvertedIndex }

type model struct {
	phase       int
	spinner     spinner.Model
	current     int
	total       int
	progressCh  chan tea.Msg
	textInput   textinput.Model
	table       table.Model
	index       *v1.InvertedIndex
	results     []v1.ScoredDoc
	currentPage int
	pageSize    int
}

func initialModel(jsonPath string) model {
	sp := spinner.New()
	sp.Spinner = spinner.Dot
	ti := textinput.New()
	ti.Placeholder = "Type to search..."
	ti.Focus()
	columns := []table.Column{
		{Title: "DocID", Width: 6},
		{Title: "Score", Width: 7},
		{Title: "Data", Width: 50},
	}
	tbl := table.New(table.WithColumns(columns))
	tbl.SetStyles(table.DefaultStyles())
	m := model{
		spinner:     sp,
		textInput:   ti,
		table:       tbl,
		progressCh:  make(chan tea.Msg, 10),
		currentPage: 0,
		pageSize:    10,
	}
	go runIndexing(jsonPath, m.progressCh)
	return m
}

func runIndexing(path string, progressCh chan tea.Msg) {
	total, err := v1.RowCount(path)
	if err != nil {
		log.Fatalf("Error counting rows: %v", err)
	}
	var cnt int
	idx, err := v1.BuildIndex(path, func(v v1.GenericRecord) error {
		cnt++
		if cnt%100000 == 0 {
			select {
			case progressCh <- indexedMsg{current: cnt, total: total}:
			default:
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}
	progressCh <- indexDoneMsg{index: idx}
}

func indexProgressCmd(ch chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		return <-ch
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(spinner.Tick, indexProgressCmd(m.progressCh), textinput.Blink)
}

func paginate(results []v1.ScoredDoc, page, size int) []v1.ScoredDoc {
	start := page * size
	if start >= len(results) {
		return []v1.ScoredDoc{}
	}
	end := int(math.Min(float64(start+size), float64(len(results))))
	return results[start:end]
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		if m.index == nil {
			m.spinner, _ = m.spinner.Update(msg)
			return m, tea.Batch(spinner.Tick, indexProgressCmd(m.progressCh))
		}
	case indexedMsg:
		m.current = msg.current
		m.total = msg.total
		return m, indexProgressCmd(m.progressCh)
	case indexDoneMsg:
		m.index = msg.index
		return m, nil
	case tea.KeyMsg:
		switch k := msg.String(); k {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "n":
			if (m.currentPage+1)*m.pageSize < len(m.results) {
				m.currentPage++

				page := paginate(m.results, m.currentPage, m.pageSize)
				rows := make([]table.Row, len(page))
				for i, sd := range page {
					rows[i] = table.Row{fmt.Sprint(sd.DocID), fmt.Sprintf("%.4f", sd.Score), fmt.Sprint(m.index.Documents[sd.DocID])}
				}
				m.table.SetRows(rows)
			}
			return m, nil
		case "p":
			if m.currentPage > 0 {
				m.currentPage--
				page := paginate(m.results, m.currentPage, m.pageSize)
				rows := make([]table.Row, len(page))
				for i, sd := range page {
					rows[i] = table.Row{fmt.Sprint(sd.DocID), fmt.Sprintf("%.4f", sd.Score), fmt.Sprint(m.index.Documents[sd.DocID])}
				}
				m.table.SetRows(rows)
			}
			return m, nil
		}
		var cmd tea.Cmd
		m.textInput, cmd = m.textInput.Update(msg)
		q := m.textInput.Value()
		if m.index != nil && len(q) > 3 {
			scored := v1.ScoreQuery(v1.NewTermQuery(q, true, 1), m.index, q)
			m.results = scored
			m.currentPage = 0
			page := paginate(scored, 0, m.pageSize)
			rows := make([]table.Row, len(page))
			for i, sd := range page {
				rows[i] = table.Row{fmt.Sprint(sd.DocID), fmt.Sprintf("%.4f", sd.Score), fmt.Sprint(m.index.Documents[sd.DocID])}
			}
			m.table.SetRows(rows)
		} else {
			m.results = nil
			m.table.SetRows(nil)
		}
		return m, cmd
	default:
		var cmd tea.Cmd
		m.textInput, cmd = m.textInput.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m model) View() string {
	searchUI := fmt.Sprintf("%s\n\n%s",
		m.textInput.View(),
		m.table.View(),
	)
	var progressInfo string
	if m.index == nil {
		progressInfo = fmt.Sprintf("Indexing: %d/%d %s", m.current, m.total, m.spinner.View())
	} else {
		progressInfo = fmt.Sprintf("Indexing complete: %d records indexed.", m.total)
	}
	pageInfo := ""
	if len(m.results) > 0 {
		totalPages := int(math.Ceil(float64(len(m.results)) / float64(m.pageSize)))
		pageInfo = fmt.Sprintf("Page %d/%d | (n) Next, (p) Previous | (q) Quit", m.currentPage+1, totalPages)
	}
	return fmt.Sprintf("%s\n\n%s\n\n%s", searchUI, progressInfo, pageInfo)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: search <json-file>")
		os.Exit(1)
	}
	jsonPath := os.Args[1]
	p := tea.NewProgram(initialModel(jsonPath), tea.WithAltScreen())
	if err := p.Start(); err != nil {
		log.Fatalf("Error running program: %v", err)
	}
}
