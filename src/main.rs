extern crate core;

use chrono::{DateTime, Utc};
use fast_image_resize as fr;
use flate2::read::GzDecoder;
use image::imageops::{overlay};
use image::{Rgb, RgbImage};
use imageproc::drawing::{draw_text_mut, text_size};
use itertools::{Itertools};
use rusttype::{Font, Scale};
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use std::fs;
use std::fs::File;
use std::io::{BufReader};
use std::num::NonZeroU32;
use std::path::PathBuf;
use structopt::StructOpt;
use num_format::{Locale, ToFormattedString};

// {"segment":233023,"num":33}
#[derive(Deserialize, Debug)]
struct Segment {
    segment: usize,
    num: usize,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Delete,
    Expire,
}

// {"bucket":"2022-09-02 15:55:00.0","operation":"delete","segment":133135,"items":[1,2,3,4,5,6]}
#[derive(Deserialize, Debug)]
struct Event {
    pub bucket: DateTime<Utc>,
    pub operation: Operation,
    pub segment: usize,
    pub items: Vec<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
enum FileState {
    // File exists
    Present,
    // Key is deleted
    DeleteMarker,
    // File completely deleted
    Expired,
    // The delete marker is gone
    DeleteMarkerDeleted,
    // Weird cases - duplicate log lines, incorrect ordering, etc etc.
    // Should only have a few hundred rows line this.
    WeirdCase,
}

impl From<&FileState> for Rgb<u8> {
    fn from(v: &FileState) -> Self {
        match v {
            FileState::Present => Rgb([0, 255, 0]),
            FileState::DeleteMarker => Rgb([255, 255, 0]),
            FileState::Expired => Rgb([255, 0, 0]),
            FileState::DeleteMarkerDeleted => Rgb([0, 0, 0]),
            FileState::WeirdCase => Rgb([0, 0, 0]),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct State {
    offsets: Vec<usize>,
    files: Vec<FileState>,
    image_size: usize,
    output_image_size: u32,
}

impl State {
    fn new(offsets: Vec<usize>, total_size: usize, output_image_size: u32) -> Self {
        let files = vec![FileState::Present; total_size];
        let length = files.len();
        let image_size = ((length as f64).sqrt() as usize) + 1;
        State {
            offsets,
            files,
            image_size,
            output_image_size,
        }
    }

    fn set_item(&mut self, segment: usize, number: usize, operation: &Operation) {
        // Set a given file to a state, described by a segment and a number.
        // Segments and numbers are 1-indexed, so we need to subtract 1 from each of them
        // Calculate the offset
        let offset = self.offsets[segment - 1];
        let idx = offset + number - 1;
        match self.files.get_mut(idx) {
            None => {
                panic!(
                    "Error! segment = {} number = {} offset = {} idx = {} len = {} state={:?}",
                    segment,
                    number,
                    offset,
                    idx,
                    self.files.len(),
                    self
                )
            }
            Some(item) => {
                match (operation, &item) {
                    // Standard flow
                    (Operation::Delete, FileState::Present) => *item = FileState::DeleteMarker,
                    (Operation::Expire, FileState::DeleteMarker) => *item = FileState::Expired,
                    (Operation::Expire, FileState::Expired) => {
                        *item = FileState::DeleteMarkerDeleted
                    }
                    // Exceptions
                    (Operation::Delete, FileState::DeleteMarker) => {
                        *item = FileState::DeleteMarkerDeleted
                    }
                    (Operation::Expire, FileState::Present) => {
                        *item = FileState::DeleteMarkerDeleted
                    }
                    // Weird?
                    (Operation::Delete, FileState::DeleteMarkerDeleted) => {
                        *item = FileState::WeirdCase
                    }
                    (Operation::Expire, FileState::DeleteMarkerDeleted) => {
                        *item = FileState::WeirdCase
                    }
                    (_, FileState::WeirdCase) => {}
                    _ => panic!("Failure: op={:?} item={:?}", operation, item),
                }
            }
        }
    }

    fn get_frame(&self) -> RgbImage {
        log::info!("Creating image...");
        // The slowest part of the whole shebang.
        let img =
            image::ImageBuffer::from_fn(self.image_size as u32, self.image_size as u32, |x, y| {
                let row_idx = y * self.image_size as u32;
                let idx = row_idx + x;
                match self.files.get(idx as usize) {
                    // I don't know how to make an Option<FileState> turn into an RGB value. Oh well.
                    None => Rgb([0, 0, 0]),
                    Some(v) => v.into(),
                }
            });
        log::info!("Resizing image...");
        // Taken from the fast-resize crate examples
        let width = NonZeroU32::new(img.width()).unwrap();
        let height = NonZeroU32::new(img.height()).unwrap();
        let src_image =
            fr::Image::from_vec_u8(width, height, img.into_raw(), fr::PixelType::U8x3).unwrap();

        // Create container for data of destination image
        let dst_width = NonZeroU32::new(self.output_image_size).unwrap();
        let dst_height = NonZeroU32::new(self.output_image_size).unwrap();
        let mut dst_image = fr::Image::new(dst_width, dst_height, src_image.pixel_type());

        // Get mutable view of destination image data
        let mut dst_view = dst_image.view_mut();
        // Create Resizer instance and resize source image
        // into buffer of destination image
        let mut resizer = fr::Resizer::new(fr::ResizeAlg::Convolution(fr::FilterType::Lanczos3));
        resizer.resize(&src_image.view(), &mut dst_view).unwrap();
        log::info!("Resized...");
        RgbImage::from_raw(self.output_image_size, self.output_image_size, dst_image.buffer().to_vec()).expect("Error converting resized")
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(parse(from_os_str))]
    segments: PathBuf,

    #[structopt(parse(from_os_str))]
    events: PathBuf,

    #[structopt(parse(from_os_str))]
    state_dir: PathBuf,

    output_size: u32,
}

// 940370485
// 940360641

fn main() {
    SimpleLogger::new().init().unwrap();

    let opt: Opt = Opt::from_args();

    let mut segments: Vec<Segment> = vec![];

    let segments_file = fs::read_dir(opt.segments).expect("Error reading segment dir");
    for segment in segments_file {
        let segment = segment.expect("Error reading file");
        let file = File::open(segment.path()).expect("Error reading file");
        let reader = BufReader::new(GzDecoder::new(file));
        let lines = serde_json::Deserializer::from_reader(reader)
            .into_iter::<Segment>()
            .map(|v| v.expect("Error reading line"));
        segments.extend(lines);
    }

    segments.sort_by_key(|v| v.segment);
    let total_files: usize = segments.iter().map(|v| v.num).sum();
    log::info!(
        "Read {} segments with {} total files",
        segments.len(),
        total_files
    );

    // let offsets = segments.iter().map(|s| s.segment).collect();
    let offsets: Vec<usize> = segments
        .iter()
        .scan(0, |acc, x| {
            let old_value = *acc;
            *acc += x.num;
            Some(old_value)
        })
        .collect();

    let mut state = State::new(offsets, total_files, opt.output_size);

    let mut event_iterators = vec![];
    let events_files = fs::read_dir(opt.events).expect("Error reading event dir");
    for event in events_files {
        let event = event.expect("Error reading file");
        log::info!("Reading event file {:?}", event.path());
        let file = File::open(event.path()).expect("Error reading file");
        let reader = BufReader::new(GzDecoder::new(file));
        let event_lines = serde_json::Deserializer::from_reader(reader)
            .into_iter::<Event>()
            .map(|v| v.expect("Error reading line"));
        event_iterators.push(Box::new(event_lines));
    }

    let font = Vec::from(include_bytes!("DejaVuSans.ttf") as &[u8]);
    let font = Font::try_from_vec(font).unwrap();

    // let mut frames = vec![];
    let items = event_iterators
        .into_iter()
        .kmerge_by(|a, b| a.bucket < b.bucket)
        .group_by(|e| e.bucket);

    let mut previous_date_time: Option<DateTime<Utc>> = None;
    let mut first_date_time: Option<DateTime<Utc>> = None;

    for (idx, (key, group)) in items.into_iter().enumerate() {
        let previous_group = match previous_date_time {
            None => key,
            Some(v) => v,
        };

        first_date_time = match first_date_time {
            None => Some(key),
            Some(v) => Some(v)
        };
        // I don't know how to make this nicer :(
        let duration_since_start = key - first_date_time.unwrap();

        // log::info!("Processing group {} = {}", key, group.count());
        log::info!("Processing group {}", key);
        let mut total_actions = 0;

        for event in group {
            total_actions += event.items.len() as i64;

            for item in event.items {
                state.set_item(event.segment, item as usize, &event.operation);
            }
        }

        let actions_per_second = total_actions.checked_div(key.timestamp() - previous_group.timestamp()).unwrap_or(0);

        let present = state
            .files
            .iter()
            .filter(|s| **s == FileState::Present)
            .count();
        let delete_marker = state
            .files
            .iter()
            .filter(|s| **s == FileState::DeleteMarker)
            .count();
        let expired = state
            .files
            .iter()
            .filter(|s| **s == FileState::Expired)
            .count();
        let delete_marker_deleted = state
            .files
            .iter()
            .filter(|s| **s == FileState::DeleteMarkerDeleted)
            .count();
        let weird_case = state
            .files
            .iter()
            .filter(|s| **s == FileState::WeirdCase)
            .count();
        log::info!("Present = {}, delete_marker = {}, expired = {}, delete_marked_deleted = {} weird_case = {}", present, delete_marker, expired, delete_marker_deleted, weird_case);
        log::info!("Per second: {}", actions_per_second);

        let mut overlay_image =
            RgbImage::from_pixel(opt.output_size, opt.output_size + 400, Rgb([255, 255, 255]));

        let scale = Scale {
            x: 45.0,
            y: 45.0,
        };

        let line_buffer = 20;
        let mut start_y = 25;

        let text_items = vec![
            format!("Hours: {}", duration_since_start.num_hours()),
            format!("Present: {}", present.to_formatted_string(&Locale::en)),
            format!("Delete Marker: {}", delete_marker.to_formatted_string(&Locale::en)),
            format!("Expired: {}", expired.to_formatted_string(&Locale::en)),
            format!("Completed: {}", delete_marker_deleted.to_formatted_string(&Locale::en)),
            format!("Per second: {}", actions_per_second.to_formatted_string(&Locale::en)),
        ];

        for item in text_items.into_iter() {
            let text_size = text_size(scale, &font, &item);
            draw_text_mut(
                &mut overlay_image,
                Rgb([0, 0, 0]),
                25,
                start_y,
                scale,
                &font,
                &item,
            );

            start_y += text_size.1 + line_buffer
        }

        let state_frame = state.get_frame();
        overlay(&mut overlay_image, &state_frame, 0, 400);
        let save_path = opt.state_dir.join(format!("{:0width$}.png", idx, width = 4));
        overlay_image.save(save_path).expect("Error saving image");

        previous_date_time = Some(key);
    }
}
