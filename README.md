# S3 deletion visualizer

## Input data

The tool takes two sets of inputs: `segments` and `events`. Both are directories containing gzip compressed JSON files.

### Segments

Segments are represented as 

```json
{"segment": 123, "num": 123}
```

### Events

Events are represented as the following structure, with each file sorted by `bucket`:

```json
{"bucket": "2022-09-02 15:55:00.0", "operation":  "delete", "segment": 123, "items":  [1, 2, 3]}
```

## Running

```shell
RUSTFLAGS="-C target-cpu=native" cargo run --release data/segments/ data/events_sorted/ data/state/ 1000
```

## Output

A series of PNG files will be written to the `state` directory, each of which are `output_size * output_size`. These 
can be converted into a gif using ImageMagik: `convert -delay 2 -loop 0 data/state/*.png animation_2.gif`
