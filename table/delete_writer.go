// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"context"
	"fmt"
	"github.com/apache/iceberg-go/config"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
)

func (t DeleteWriteTask) GenerateDeleteFileName(extension string) string {
	return fmt.Sprintf("00000-%d-%s-deletes.%s", t.ID, t.Uuid, extension)
}

// EqualityDeleteTask represents a task to write equality deletes
type DeleteWriteTask struct {
	WriteTask
	content          iceberg.ManifestEntryContent
	equalityFieldIds []int
}

// writeDeleteFile writes a delete file and returns a DeleteFile
func (w *writer) writeDeleteFile(ctx context.Context, task DeleteWriteTask) (iceberg.DataFile, error) {
	defer func() {
		for _, b := range task.Batches {
			b.Release()
		}
	}()

	batches := make([]arrow.Record, len(task.Batches))
	for i, b := range task.Batches {
		rec, err := ToRequestedSchema(ctx, w.fileSchema,
			task.Schema, b, false, true, false)
		if err != nil {
			return nil, err
		}
		batches[i] = rec
	}

	statsCols, err := computeStatsPlan(w.fileSchema, w.meta.props)
	if err != nil {
		return nil, err
	}

	// Generate file path for the delete file
	filePath := w.loc.NewDataLocation(task.GenerateDeleteFileName(w.format.Extension()))

	// Write the delete file
	return w.format.WriteDeleteFile(ctx, iceberg.EntryContentEqDeletes, w.fs, internal.WriteFileInfo{
		FileSchema: w.fileSchema,
		FileName:   filePath,
		StatsCols:  statsCols,
		WriteProps: w.props,
	}, task.Batches, task.equalityFieldIds, &task.SortOrderID)
}

func writeDeleteFiles(ctx context.Context, rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, tasks iter.Seq[DeleteWriteTask]) iter.Seq2[iceberg.DataFile, error] {
	w, err := createWriter(rootLocation, fs, meta)
	if err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	nworkers := config.EnvConfig.MaxWorkers

	return internal.MapExec(nworkers, tasks, func(t DeleteWriteTask) (iceberg.DataFile, error) {
		return w.writeDeleteFile(ctx, t)
	})
}
