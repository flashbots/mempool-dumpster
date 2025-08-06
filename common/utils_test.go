package common

import (
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func TestBytesFormat(t *testing.T) {
	n := uint64(795025173)

	s := humanize.Bytes(n)
	require.Equal(t, "795 MB", s)

	s = humanize.IBytes(n)
	require.Equal(t, "758 MiB", s)

	s = HumanBytes(n)
	require.Equal(t, "758 MB", s)

	s = HumanBytes(n * 10)
	require.Equal(t, "7.4 GB", s)

	s = HumanBytes(n / 1000)
	require.Equal(t, "776 KB", s)
}

func TestInt64DiffPercentFmtC(t *testing.T) {
	require.Equal(t, "99.9%", Int64DiffPercentFmt(40614, 40624, 1))
	require.Equal(t, "99.97%", Int64DiffPercentFmt(40614, 40624, 2))

	require.Equal(t, "99.97%", IntDiffPercentFmt(40614, 40624, 2))
	require.Equal(t, "99.9%", IntDiffPercentFmt(40614, 40624, 1))
}

func TestDateFmtDay(t *testing.T) {
	timestampMs := int64(1619712000000)
	_t := time.Unix(timestampMs/1000, 0).UTC()
	require.Equal(t, "2021-04-29", FmtDateDay(_t))
	require.Equal(t, "2021-04-29 16:00:00", FmtDateDayTime(_t))

	ts1 := int64(1619712000187)
	ts2 := int64(1619712004321)
	t1 := time.Unix(ts1, 0)
	t2 := time.Unix(ts2, 28341)
	d := t2.Sub(t1)
	require.Equal(t, "1h 8m 54s", FmtDuration(d))
}

func TestParseDateString(t *testing.T) {
	dateStr := "2021-04-29"
	tm, err := ParseDateString(dateStr)
	require.NoError(t, err)
	require.Equal(t, "2021-04-29 00:00:00 +0000 UTC", tm.String())

	dateStr = "2021-04-29 16:00:00"
	tm, err = ParseDateString(dateStr)
	require.NoError(t, err)
	require.Equal(t, "2021-04-29 16:00:00 +0000 UTC", tm.String())

	dateStr = "2021-04-29T16:00:00Z"
	tm, err = ParseDateString(dateStr)
	require.NoError(t, err)
	require.Equal(t, "2021-04-29 16:00:00 +0000 UTC", tm.String())

	// Invalid date string
	_, err = ParseDateString("invalid-date")
	require.Error(t, err)
}
