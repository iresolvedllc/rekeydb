//! Geohash encoding and search geometry, ported from KeyDB's `geohash.c`
//! and `geohash_helper.cpp`.
//!
//! GEO commands store 52-bit interleaved geohash values as sorted-set
//! scores, so all of the arithmetic here has to match the C implementation
//! bit for bit.

pub const GEO_LAT_MIN: f64 = -85.05112878;
pub const GEO_LAT_MAX: f64 = 85.05112878;
pub const GEO_LONG_MIN: f64 = -180.0;
pub const GEO_LONG_MAX: f64 = 180.0;
pub const GEO_STEP_MAX: u8 = 26;

const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
const MERCATOR_MAX: f64 = 20037726.37;
const D_R: f64 = std::f64::consts::PI / 180.0;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeoHashBits {
    pub bits: u64,
    pub step: u8,
}

impl GeoHashBits {
    pub fn is_zero(self) -> bool {
        self.bits == 0 && self.step == 0
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GeoHashRange {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GeoHashArea {
    pub hash: GeoHashBits,
    pub longitude: GeoHashRange,
    pub latitude: GeoHashRange,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GeoHashNeighbors {
    pub north: GeoHashBits,
    pub east: GeoHashBits,
    pub west: GeoHashBits,
    pub south: GeoHashBits,
    pub north_east: GeoHashBits,
    pub south_east: GeoHashBits,
    pub north_west: GeoHashBits,
    pub south_west: GeoHashBits,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GeoHashRadius {
    pub hash: GeoHashBits,
    pub neighbors: GeoHashNeighbors,
    pub area: GeoHashArea,
}

#[derive(Debug, Clone, Copy)]
pub struct GeoShape {
    pub circular: bool,
    pub xy: (f64, f64),
    pub conversion: f64,
    pub radius: f64,
    pub width: f64,
    pub height: f64,
    pub bounds: [f64; 4],
}

impl Default for GeoShape {
    fn default() -> Self {
        Self {
            circular: true,
            xy: (0.0, 0.0),
            conversion: 1.0,
            radius: 0.0,
            width: 0.0,
            height: 0.0,
            bounds: [0.0; 4],
        }
    }
}

fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
    ];
    const S: [u32; 5] = [1, 2, 4, 8, 16];

    let mut x = u64::from(xlo);
    let mut y = u64::from(ylo);
    for index in (0..5).rev() {
        x = (x | (x << S[index])) & B[index];
        y = (y | (y << S[index])) & B[index];
    }
    x | (y << 1)
}

fn deinterleave64(interleaved: u64) -> u64 {
    const B: [u64; 6] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
        0x0000_0000_FFFF_FFFF,
    ];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];

    let mut x = interleaved;
    let mut y = interleaved >> 1;
    for index in 0..=5 {
        x = (x | (x >> S[index])) & B[index];
        y = (y | (y >> S[index])) & B[index];
    }
    x | (y << 32)
}

/// Encode a longitude/latitude pair into a geohash of `step` bits per axis.
pub fn encode(longitude: f64, latitude: f64, step: u8) -> Option<GeoHashBits> {
    if step == 0 || step > 32 {
        return None;
    }
    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&longitude)
        || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&latitude)
    {
        return None;
    }
    let lat_offset = (latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);
    let long_offset = (longitude - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);
    let scale = (1u64 << step) as f64;
    let lat_offset = lat_offset * scale;
    let long_offset = long_offset * scale;
    Some(GeoHashBits {
        bits: interleave64(lat_offset as u32, long_offset as u32),
        step,
    })
}

/// Encode using the conventional geohash ranges (latitude -90..90).
///
/// GEO stores coordinates using a slightly different latitude range, so
/// `GEOHASH` decodes the stored point and re-encodes it with these ranges.
pub fn encode_standard(longitude: f64, latitude: f64, step: u8) -> Option<GeoHashBits> {
    if step == 0 || step > 32 {
        return None;
    }
    if !(-180.0..=180.0).contains(&longitude) || !(-90.0..=90.0).contains(&latitude) {
        return None;
    }
    let lat_offset = (latitude + 90.0) / 180.0;
    let long_offset = (longitude + 180.0) / 360.0;
    let scale = (1u64 << step) as f64;
    Some(GeoHashBits {
        bits: interleave64((lat_offset * scale) as u32, (long_offset * scale) as u32),
        step,
    })
}

/// Decode a geohash into the WGS84 bounding box it covers.
pub fn decode(hash: GeoHashBits) -> Option<GeoHashArea> {
    if hash.is_zero() {
        return None;
    }
    let step = hash.step;
    let hash_sep = deinterleave64(hash.bits);
    let lat_scale = GEO_LAT_MAX - GEO_LAT_MIN;
    let long_scale = GEO_LONG_MAX - GEO_LONG_MIN;
    let ilat = hash_sep as u32;
    let ilon = (hash_sep >> 32) as u32;
    let divisor = (1u64 << step) as f64;
    Some(GeoHashArea {
        hash,
        latitude: GeoHashRange {
            min: GEO_LAT_MIN + (f64::from(ilat) / divisor) * lat_scale,
            max: GEO_LAT_MIN + (f64::from(ilat + 1) / divisor) * lat_scale,
        },
        longitude: GeoHashRange {
            min: GEO_LONG_MIN + (f64::from(ilon) / divisor) * long_scale,
            max: GEO_LONG_MIN + (f64::from(ilon + 1) / divisor) * long_scale,
        },
    })
}

/// Decode a stored sorted-set score (52-bit geohash) to a center point.
pub fn decode_score(score: f64) -> Option<(f64, f64)> {
    let hash = GeoHashBits {
        bits: score as u64,
        step: GEO_STEP_MAX,
    };
    let area = decode(hash)?;
    Some(area_center(&area))
}

fn area_center(area: &GeoHashArea) -> (f64, f64) {
    let longitude =
        ((area.longitude.min + area.longitude.max) / 2.0).clamp(GEO_LONG_MIN, GEO_LONG_MAX);
    let latitude = ((area.latitude.min + area.latitude.max) / 2.0).clamp(GEO_LAT_MIN, GEO_LAT_MAX);
    (longitude, latitude)
}

pub fn align52_bits(hash: GeoHashBits) -> u64 {
    hash.bits << (52 - u32::from(hash.step) * 2)
}

/// Compute the inclusive / exclusive score bounds covering a geohash box.
pub fn scores_of_box(hash: GeoHashBits) -> (u64, u64) {
    let min = align52_bits(hash);
    let mut upper = hash;
    upper.bits = upper.bits.wrapping_add(1);
    (min, align52_bits(upper))
}

fn move_x(hash: &mut GeoHashBits, direction: i8) {
    if direction == 0 {
        return;
    }
    let x = hash.bits & 0xAAAA_AAAA_AAAA_AAAA;
    let y = hash.bits & 0x5555_5555_5555_5555;
    let shift = 64 - u32::from(hash.step) * 2;
    let zz = 0x5555_5555_5555_5555u64 >> shift;
    let mut x = if direction > 0 {
        x.wrapping_add(zz.wrapping_add(1))
    } else {
        (x | zz).wrapping_sub(zz.wrapping_add(1))
    };
    x &= 0xAAAA_AAAA_AAAA_AAAAu64 >> shift;
    hash.bits = x | y;
}

fn move_y(hash: &mut GeoHashBits, direction: i8) {
    if direction == 0 {
        return;
    }
    let x = hash.bits & 0xAAAA_AAAA_AAAA_AAAA;
    let y = hash.bits & 0x5555_5555_5555_5555;
    let shift = 64 - u32::from(hash.step) * 2;
    let zz = 0xAAAA_AAAA_AAAA_AAAAu64 >> shift;
    let mut y = if direction > 0 {
        y.wrapping_add(zz.wrapping_add(1))
    } else {
        (y | zz).wrapping_sub(zz.wrapping_add(1))
    };
    y &= 0x5555_5555_5555_5555u64 >> shift;
    hash.bits = x | y;
}

pub fn neighbors(hash: GeoHashBits) -> GeoHashNeighbors {
    let mut result = GeoHashNeighbors {
        north: hash,
        east: hash,
        west: hash,
        south: hash,
        north_east: hash,
        south_east: hash,
        north_west: hash,
        south_west: hash,
    };
    move_x(&mut result.east, 1);
    move_x(&mut result.west, -1);
    move_y(&mut result.south, -1);
    move_y(&mut result.north, 1);
    move_x(&mut result.north_west, -1);
    move_y(&mut result.north_west, 1);
    move_x(&mut result.north_east, 1);
    move_y(&mut result.north_east, 1);
    move_x(&mut result.south_east, 1);
    move_y(&mut result.south_east, -1);
    move_x(&mut result.south_west, -1);
    move_y(&mut result.south_west, -1);
    result
}

pub fn estimate_steps_by_radius(mut range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut step = 1;
    while range_meters < MERCATOR_MAX {
        range_meters *= 2.0;
        step += 1;
    }
    step -= 2;
    if lat > 66.0 || lat < -66.0 {
        step -= 1;
        if lat > 80.0 || lat < -80.0 {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

fn deg_rad(angle: f64) -> f64 {
    angle * D_R
}

fn rad_deg(angle: f64) -> f64 {
    angle / D_R
}

pub fn bounding_box(shape: &mut GeoShape) -> [f64; 4] {
    let (longitude, latitude) = shape.xy;
    let height = shape.conversion
        * if shape.circular {
            shape.radius
        } else {
            shape.height / 2.0
        };
    let width = shape.conversion
        * if shape.circular {
            shape.radius
        } else {
            shape.width / 2.0
        };
    let lat_delta = rad_deg(height / EARTH_RADIUS_IN_METERS);
    let long_delta_top =
        rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude + lat_delta).cos());
    let long_delta_bottom =
        rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude - lat_delta).cos());
    let southern_hemisphere = latitude < 0.0;
    let bounds = [
        if southern_hemisphere {
            longitude - long_delta_bottom
        } else {
            longitude - long_delta_top
        },
        latitude - lat_delta,
        if southern_hemisphere {
            longitude + long_delta_bottom
        } else {
            longitude + long_delta_top
        },
        latitude + lat_delta,
    ];
    shape.bounds = bounds;
    bounds
}

/// Calculate the search center box plus its eight neighbors.
pub fn calculate_areas(shape: &mut GeoShape) -> GeoHashRadius {
    let bounds = bounding_box(shape);
    let (min_lon, min_lat, max_lon, max_lat) = (bounds[0], bounds[1], bounds[2], bounds[3]);
    let (longitude, latitude) = shape.xy;
    let mut radius_meters = if shape.circular {
        shape.radius
    } else {
        ((shape.width / 2.0).powi(2) + (shape.height / 2.0).powi(2)).sqrt()
    };
    radius_meters *= shape.conversion;

    let mut steps = estimate_steps_by_radius(radius_meters, latitude);
    let mut hash = encode(longitude, latitude, steps).unwrap_or_default();
    let mut neighbor_boxes = neighbors(hash);
    let mut area = decode(hash).unwrap_or_default();

    let mut decrease_step = false;
    {
        let north = decode(neighbor_boxes.north).unwrap_or_default();
        let south = decode(neighbor_boxes.south).unwrap_or_default();
        let east = decode(neighbor_boxes.east).unwrap_or_default();
        let west = decode(neighbor_boxes.west).unwrap_or_default();
        if distance(longitude, latitude, longitude, north.latitude.max) < radius_meters {
            decrease_step = true;
        }
        if distance(longitude, latitude, longitude, south.latitude.min) < radius_meters {
            decrease_step = true;
        }
        if distance(longitude, latitude, east.longitude.max, latitude) < radius_meters {
            decrease_step = true;
        }
        if distance(longitude, latitude, west.longitude.min, latitude) < radius_meters {
            decrease_step = true;
        }
    }

    if steps > 1 && decrease_step {
        steps -= 1;
        hash = encode(longitude, latitude, steps).unwrap_or_default();
        neighbor_boxes = neighbors(hash);
        area = decode(hash).unwrap_or_default();
    }

    if steps >= 2 {
        if area.latitude.min < min_lat {
            neighbor_boxes.south = GeoHashBits::default();
            neighbor_boxes.south_west = GeoHashBits::default();
            neighbor_boxes.south_east = GeoHashBits::default();
        }
        if area.latitude.max > max_lat {
            neighbor_boxes.north = GeoHashBits::default();
            neighbor_boxes.north_east = GeoHashBits::default();
            neighbor_boxes.north_west = GeoHashBits::default();
        }
        if area.longitude.min < min_lon {
            neighbor_boxes.west = GeoHashBits::default();
            neighbor_boxes.south_west = GeoHashBits::default();
            neighbor_boxes.north_west = GeoHashBits::default();
        }
        if area.longitude.max > max_lon {
            neighbor_boxes.east = GeoHashBits::default();
            neighbor_boxes.south_east = GeoHashBits::default();
            neighbor_boxes.north_east = GeoHashBits::default();
        }
    }

    GeoHashRadius {
        hash,
        neighbors: neighbor_boxes,
        area,
    }
}

pub fn distance(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lat1r = deg_rad(lat1d);
    let lon1r = deg_rad(lon1d);
    let lat2r = deg_rad(lat2d);
    let lon2r = deg_rad(lon2d);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let v = ((lon2r - lon1r) / 2.0).sin();
    2.0 * EARTH_RADIUS_IN_METERS * (u * u + lat1r.cos() * lat2r.cos() * v * v).sqrt().asin()
}

pub fn distance_if_in_circle(x1: f64, y1: f64, x2: f64, y2: f64, radius: f64) -> Option<f64> {
    let distance = distance(x1, y1, x2, y2);
    (distance <= radius).then_some(distance)
}

pub fn distance_if_in_rectangle(
    width_m: f64,
    height_m: f64,
    x1: f64,
    y1: f64,
    x2: f64,
    y2: f64,
) -> Option<f64> {
    let lon_distance = distance(x2, y2, x1, y2);
    let lat_distance = distance(x2, y2, x2, y1);
    if lon_distance > width_m / 2.0 || lat_distance > height_m / 2.0 {
        return None;
    }
    Some(distance(x1, y1, x2, y2))
}
