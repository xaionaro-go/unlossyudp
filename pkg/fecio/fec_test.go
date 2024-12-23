package fecio

import (
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/facebookincubator/go-belt/tool/logger/implementation/logrus"
	"github.com/facebookincubator/go-belt/tool/logger/types"
	"github.com/mixer/clock"
	"github.com/stretchr/testify/require"
)

type dummyWriter struct {
	backendWriter io.WriteCloser
	writeFunc     func([]byte) (int, error)
}

var _ io.Writer = (*dummyWriter)(nil)

func (r *dummyWriter) Write(p []byte) (int, error) {
	return r.writeFunc(p)
}

func (r *dummyWriter) Close() error {
	return r.backendWriter.Close()
}

func newReadAndWriter(
	t require.TestingT,
) (*FECReader, *FECWriter, *dummyWriter) {
	nr, nw := io.Pipe()

	dw := &dummyWriter{
		backendWriter: nw,
	}

	fr, err := NewFECReader(nr, true, 1500, 4)
	require.NoError(t, err)

	fw, err := NewFECWriter(
		dw,
		[]RedundancyConfiguration{
			{
				DataPackets:       3,
				RedundancyPackets: 2,
			},
			{
				DataPackets:       1,
				RedundancyPackets: 1,
			},
		},
		time.Millisecond,
		1500,
	)
	require.NoError(t, err)

	return fr, fw, dw
}

func TestFEC(t *testing.T) {
	Logger = logrus.Default()
	Logger = Logger.WithLevel(logger.LevelTrace)
	logger.Default = func() types.Logger {
		return Logger
	}
	mockClock := clock.NewMockClock(time.Unix(0, 0))
	//myClock = mockClock

	Logger.Infof("opening the pipe")

	messages := [][]byte{
		{8, 1},
		{8, 2},
		{8, 3},
		{8, 4},
		{8, 5},
	}

	t.Run("simple-Read-Write", func(t *testing.T) {
		fr, fw, dw := newReadAndWriter(t)

		dw.writeFunc = func(message []byte) (int, error) {
			Logger.Debugf("writing to the backend: %X", message)
			n, err := dw.backendWriter.Write(message)
			Logger.Debugf("/writing to the backend: %X: %d %v", message, n, err)
			runtime.Gosched()
			return n, err
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, message := range messages {
				n, err := fw.Write(message)
				Logger.Debugf("wrote message %X", message)
				require.NoError(t, err)
				require.Equal(t, len(message), n)
			}
			mockClock.SetTime(myClock.Now().Add(2 * time.Second))
			Logger.Debugf("ticked the clock")
		}()

		var transmittedMessages [][]byte
		for i := 0; i < len(messages); i++ {
			buf := make([]byte, 3000)
			n, err := fr.Read(buf)
			require.NoError(t, err)
			require.Equal(t, 2, n, buf[:n])
			message := buf[:n]
			Logger.Debugf("read message %X", message)
			transmittedMessages = append(transmittedMessages, message)
		}

		require.Equal(t, messages, transmittedMessages)

		Logger.Infof("closing the pipe")
		fr.Close()
		fw.Close()

		Logger.Infof("waiting for the SyncGroup")
		wg.Wait()
		time.Sleep(time.Millisecond)
	})

	t.Run("lossy-Read-Write", func(t *testing.T) {
		fr, fw, dw := newReadAndWriter(t)

		writeCount := 0
		dw.writeFunc = func(message []byte) (int, error) {
			writeCount++
			if writeCount == 1 {
				return len(message), nil
			}
			Logger.Debugf("writing to the backend: %X", message)
			n, err := dw.backendWriter.Write(message)
			Logger.Debugf("/writing to the backend: %X: %d %v", message, n, err)
			runtime.Gosched()
			return n, err
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, message := range messages {
				n, err := fw.Write(message)
				Logger.Debugf("wrote message %X", message)
				require.NoError(t, err)
				require.Equal(t, len(message), n)
			}
			mockClock.SetTime(myClock.Now().Add(2 * time.Second))
			Logger.Debugf("ticked the clock")
		}()

		var transmittedMessages [][]byte
		for i := 0; i < len(messages); i++ {
			buf := make([]byte, 3000)
			n, err := fr.Read(buf)
			require.NoError(t, err)
			require.Equal(t, 2, n, buf[:n])
			message := buf[:n]
			Logger.Debugf("read message %X", message)
			transmittedMessages = append(transmittedMessages, message)
		}

		require.Equal(t, messages, transmittedMessages)

		Logger.Infof("closing the pipe")
		fr.Close()
		fw.Close()

		Logger.Infof("waiting for the SyncGroup")
		wg.Wait()
		time.Sleep(time.Millisecond)
	})
}

func BenchmarkFEC(b *testing.B) {
	Logger = Logger.WithLevel(logger.LevelDebug)
	myClock = clock.DefaultClock{}
	nr, nw := io.Pipe()

	dw := &dummyWriter{
		backendWriter: nw,
	}

	fr, err := NewFECReader(nr, true, 1500, 4)
	require.NoError(b, err)

	fw, err := NewFECWriter(
		dw,
		[]RedundancyConfiguration{
			{
				DataPackets:       3,
				RedundancyPackets: 2,
			},
			{
				DataPackets:       1,
				RedundancyPackets: 1,
			},
		},
		time.Second,
		1500,
	)
	require.NoError(b, err)
	defer func() {
		fr.Close()
		fw.Close()
	}()

	messages := make([][]byte, 1)
	for idx := range messages {
		messages[idx] = make([]byte, 100)
	}

	dw.writeFunc = dw.backendWriter.Write

	go func() {
		for {
			for _, message := range messages {
				n, err := fw.Write(message)
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) {
					return
				}
				require.NoError(b, err)
				require.Equal(b, len(message), n)
			}
		}
	}()

	buf := make([]byte, 3000)
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(100)
	for i := 0; i < b.N; i++ {
		_, err := fr.Read(buf)
		require.NoError(b, err)
	}
}
