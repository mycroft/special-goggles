/*
** Apache log parser
**/
use std::fs;
use std::collections::HashMap;

use chrono::DateTime;
use regex::Regex;

#[derive(Clone, Debug)]
struct Record {
    ts: u32,
    uid: String,
    slug: String
}

use std::io::prelude::*;
use flate2::read::GzDecoder;

fn read_contents(filepath: &str) -> Result<String, String> {
    let content = match fs::read(filepath) {
        Err(x) => return Err(format!("Could not read file: {}", x)),
        Ok(content) => content,
    };

    let mut d = GzDecoder::new(&content[..]);
    let mut s = String::new();

    match d.read_to_string(&mut s) {
        Err(_x) => {
            // fallback to non-gzip
            return Ok(String::from_utf8(content).unwrap())
        },
        Ok(content) => content,
    };

    Ok(s)
}

fn extract_file_info(filepath: &str) -> Result<HashMap<String, Record>, String> {
    let mut dict : HashMap<String, Record> = HashMap::new();

    let rx = match Regex::new(r"\[([^\]]+)\] .GET /observabilityapp/d/([^/]+)/([^ ?]+)") {
        Err(err) => {
            return Err(format!("Could not compile regular expression: {}", err).to_string());
        },
        Ok(rx) => rx,
    };

    let contents = match read_contents(filepath) {
        Err(err) => {
            return Err(format!("Could not read file: {:?}", err));
        }, 
        Ok(contents) => contents
    };

    for line in contents.lines() {
        let caps = match rx.captures(&line) {
            None => continue,
            Some(caps) => caps,
        };

        if caps[2].len() != 9 {
            println!("Invalid record: {}", &caps[2]);
            continue;
        }

        let ts = match DateTime::parse_from_str(&caps[1], "%d/%h/%Y:%H:%M:%S %z") {
            Err(err) => {
                return Err(format!("Could not parse timestamp: {:?}", err));
            },
            Ok(ts) => ts,
        };

        // println!("TS:{} UID:{} SLUG:{}", ts, &caps[2], &caps[3]);

        dict.insert(
            caps[2].to_string(),
            Record {
                ts: ts.timestamp() as u32,
                uid: caps[2].to_string(),
                slug: caps[3].to_string()
            }
        );
    }

    Ok(dict)
}

fn parse_dir(dirpath: &str) -> Result<HashMap<String, Record>, String> {
    let mut final_dict : HashMap<String, Record> = HashMap::new();

    let dir_iter = match fs::read_dir(&dirpath) {
        Err(x) => return Err(format!("Failed to open directory: {}", x)),
        Ok(it) => it,
    };

    for entry in dir_iter {
        let entry = match entry {
            Err(x) => return Err(format!("Failed to retrieve entry info: {:?}", x)),
            Ok(entry) => entry,
        };

        let path = entry.path().into_os_string().into_string().unwrap();

        println!("Parsing {}...", &path);

        let lst = match extract_file_info(&path.as_str()) {
            Ok(vals) => vals,
            Err(err) => {
                return Err(format!("Could not extract data: {:?}", err));
            }
        };

        for (k, rec) in lst {
            if !final_dict.contains_key(&k) {
                final_dict.insert(
                    k.clone(),
                    rec.clone(),
                );

                continue;
            }

            let entry = final_dict.get_mut(&k).unwrap();

            if rec.ts > entry.ts {
                entry.ts = rec.ts;
            }
        }
    }

    Ok(final_dict)
}

fn main() {
    let dict = match parse_dir("/home/mycroft/tmp/httpd/") {
        Err(x) => {
            println!("failed to parse logs: {}", x);
            return;
        }
        Ok(dict) => dict,
    };

    for (_, elem) in dict {
        println!("UID: {} (slug: {}) TS: {}",
            elem.uid,
            elem.slug,
            elem.ts,
        );
    }
}
