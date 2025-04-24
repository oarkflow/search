package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/oarkflow/search/v1"
)

// messages for indexing progress
// indexDoneMsg signals completion; no index payload since we build in place
type indexedMsg struct{ current, total int }
type indexDoneMsg struct{ index *v1.InvertedIndex }

// debounced search message
type searchMsg string

type model struct {
	// indexing state
	indexing   bool
	spinner    spinner.Model
	current    int
	total      int
	progressCh chan tea.Msg

	// search input
	textInput    textinput.Model
	pendingQuery string

	// results table
	table       table.Model
	index       *v1.InvertedIndex
	results     []v1.ScoredDoc
	currentPage int
	pageSize    int
}

func initialModel(jsonPath string) *model {
	sp := spinner.New()
	sp.Spinner = spinner.Dot

	ti := textinput.New()
	ti.Placeholder = "Type to search (min 3 chars)..."
	ti.Focus()

	columns := []table.Column{
		{Title: "DocID", Width: 6},
		{Title: "Score", Width: 7},
		{Title: "Data", Width: 1000},
	}
	tbl := table.New(table.WithColumns(columns))
	tbl.SetStyles(table.DefaultStyles())
	tbl.SetHeight(10)

	m := &model{
		index:      v1.NewIndex(),
		indexing:   true,
		spinner:    sp,
		progressCh: make(chan tea.Msg, 1),
		textInput:  ti,
		table:      tbl,
		pageSize:   10,
	}
	// start indexing, passing the index pointer
	go m.runIndexing(jsonPath, m.progressCh)
	return m
}

func (m *model) runIndexing(path string, progressCh chan tea.Msg) {
	// get total rows
	total, err := v1.RowCount(path)
	if err != nil {
		log.Fatalf("Error counting rows: %v", err)
	}
	var count int

	// build index incrementally: callback adds each record
	_, err = m.index.Build(path, func(rec v1.GenericRecord) error {
		count++
		if count%100000 == 0 {
			select {
			case progressCh <- indexedMsg{current: count, total: total}:
			default:
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}

	// final progress
	progressCh <- indexedMsg{current: count, total: total}
	// signal indexing done
	progressCh <- indexDoneMsg{index: m.index}
}

func nextProgressMsg(ch chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		return <-ch
	}
}

func debounceSearch(q string) tea.Cmd {
	return tea.Tick(time.Millisecond*200, func(t time.Time) tea.Msg {
		return searchMsg(q)
	})
}

func paginate(results []v1.ScoredDoc, page, size int) []v1.ScoredDoc {
	start := page * size
	if start >= len(results) {
		return nil
	}
	end := int(math.Min(float64(start+size), float64(len(results))))
	return results[start:end]
}

func (m *model) Init() tea.Cmd {
	return tea.Batch(
		spinner.Tick,
		nextProgressMsg(m.progressCh),
	)
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// global quit on Ctrl+C
	if key, ok := msg.(tea.KeyMsg); ok {
		if key.Type == tea.KeyCtrlC {
			return m, tea.Quit
		}
		// pagination on Ctrl+N / Ctrl+P
		if key.Type == tea.KeyCtrlN && len(m.results) > 0 {
			if (m.currentPage+1)*m.pageSize < len(m.results) {
				m.currentPage++
				var rows []table.Row
				for _, sd := range paginate(m.results, m.currentPage, m.pageSize) {
					rows = append(rows, table.Row{fmt.Sprint(sd.DocID), fmt.Sprintf("%.4f", sd.Score), fmt.Sprint(m.index.Documents[sd.DocID])})
				}
				m.table.SetRows(rows)
			}
			return m, nil
		}
		if key.Type == tea.KeyCtrlP && m.currentPage > 0 {
			m.currentPage--
			var rows []table.Row
			for _, sd := range paginate(m.results, m.currentPage, m.pageSize) {
				rows = append(rows, table.Row{fmt.Sprint(sd.DocID), fmt.Sprintf("%.4f", sd.Score), fmt.Sprint(m.index.Documents[sd.DocID])})
			}
			m.table.SetRows(rows)
			return m, nil
		}
	}

	switch msg := msg.(type) {
	case spinner.TickMsg:
		if m.indexing {
			m.spinner, _ = m.spinner.Update(msg)
			return m, tea.Batch(spinner.Tick, nextProgressMsg(m.progressCh))
		}
		return m, nil

	case indexedMsg:
		m.current = msg.current
		m.total = msg.total
		return m, nextProgressMsg(m.progressCh)

	case indexDoneMsg:
		m.indexing = false
		// don't reset focus; input was already focused
		return m, nil

	case searchMsg:
		if m.index == nil {
			m.table.SetRows([]table.Row{{"N/A", "N/A", "Indexing in progress..."}})
			return m, nil
		}
		q := string(msg)
		if len(q) >= 1 {
			scored := v1.ScoreQuery(v1.NewTermQuery(q, true, 1), m.index, q)
			m.results = scored
			m.currentPage = 0
			var rows []table.Row
			for _, sd := range paginate(scored, 0, m.pageSize) {
				rows = append(rows, table.Row{fmt.Sprint(sd.DocID), fmt.Sprintf("%.4f", sd.Score), fmt.Sprint(m.index.Documents[sd.DocID])})
			}
			m.table.SetRows(rows)
		} else {
			m.results = nil
			m.table.SetRows(nil)
		}
		return m, nil

	case tea.KeyMsg:
		// text input updates
		var cmd tea.Cmd
		m.textInput, cmd = m.textInput.Update(msg)
		q := m.textInput.Value()
		if len(q) >= 3 && q != m.pendingQuery {
			m.pendingQuery = q
			return m, debounceSearch(q)
		}
		if len(q) < 3 {
			m.results = nil
			m.table.SetRows(nil)
		}
		return m, cmd

	default:
		return m, nil
	}
}

func (m *model) View() string {
	// always show input & table
	searchUI := fmt.Sprintf("%s\n\n%s", m.textInput.View(), m.table.View())

	// progress or summary
	var progress string
	if m.indexing {
		progress = fmt.Sprintf("Indexing %d/%d %s", m.current, m.total, m.spinner.View())
	} else {
		progress = fmt.Sprintf("Indexing complete: %d records indexed.", m.total)
	}

	// show pagination instructions
	var pageInfo string
	if len(m.results) > 0 {
		totalPages := int(math.Ceil(float64(len(m.results)) / float64(m.pageSize)))
		pageInfo = "Use Ctrl+N/Ctrl+P to navigate pages. Ctrl+C to quit."
		pageInfo += fmt.Sprintf(" (Page %d/%d)", m.currentPage+1, totalPages)
	} else {
		pageInfo = "Type at least 3 characters to search. Ctrl+C to quit."
	}

	return fmt.Sprintf("%s\n\n%s\n\n%s", searchUI, progress, pageInfo)
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
