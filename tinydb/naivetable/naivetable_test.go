package naivetable

import (
	"crypto/rand"
	"encoding/binary"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"math"

	"testing"
)

func generateRandomString(min, max int, mu float64) string {
	// randNormFloat64 generates a normally distributed float64 in range (-1, 1)
	randNormFloat64 := func() float64 {
		var u uint64
		_ = binary.Read(rand.Reader, binary.BigEndian, &u)
		return 2*(float64(u)/float64(1<<64)) - 1
	}

	sigma := float64(max-min) / 4.0
	length := int(mu + randNormFloat64()*sigma)
	length = int(math.Max(float64(min), math.Min(float64(max), float64(length))))

	// Generate a random string, consisting of ascii lowercase chars, of the determined length
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	for i := range bytes {
		bytes[i] = 'a' + (bytes[i] % 26)
	}
	return string(bytes)
}

// spawnRows generates a slice of rows of the specified amount, count.
// The string fields vary in length, according to their specified distribution
func spawnRows(count int) []tinydb.Row {
	rows := make([]tinydb.Row, count, count)

	for i, _ := range rows {
		rows[i].Id = i
		rows[i].Username = generateRandomString(8, 40, 12)
		rows[i].Email = generateRandomString(14, 52, 20) // emails typically have an additional @gmail.com, for example
	}

	return rows
}

// TestSpawnRows checks that the row-spawning process is behaving as expected, so it can be used
// in other tests
func TestSpawnRows(t *testing.T) {
	rows := spawnRows(10_000)

	for i, r := range rows {
		if i != r.Id {
			t.Error("unique, non-zero row.Ids expected")
			t.FailNow()
		}
		if r.Username == "" {
			t.Error("username is blank")
			t.FailNow()
		}
		if r.Email == "" {
			t.Error("email is blank")
			t.FailNow()
		}
	}
}
