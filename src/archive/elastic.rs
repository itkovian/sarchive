/*
Copyright 2019 Andy Georges <itkovian+sarchive@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

//use futures::future::Future;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::io::Error;
use super::Archive;
use crate::slurm::{SlurmJobEntry};

//use elastic::http::header::{self, AUTHORIZATION, HeaderValue};
use elastic::client::{SyncClient, SyncClientBuilder};


pub struct ElasticArchive {
    client: SyncClient,
    index: String
}


fn create_index(client: &SyncClient, index_name: String) -> Result<(), Error> {

    /*let body = json!({
        "mappings": {
            JobInfo::partial_static_index(): JobInfo::partial_index_mapping()
        }
    });
    */
    client.index(index_name)
        .create()
     //    .body(body.to_string())
        .send().unwrap();

    Ok(())
}


impl ElasticArchive {
    pub fn new(host: &str, port: u16, index: String) -> Self {
        let client = SyncClientBuilder::new()
                .sniff_nodes(format!("http://{host}:{port}", host=host, port=port))  // TODO: use a pool for serde
                .build().unwrap();

        let response = client.index(index.to_owned()).exists().send().unwrap();
        if !response.exists() {
            create_index(&client, index.to_owned());
        }
        ElasticArchive {
            client: client,
            index: index.to_owned(),
        }
    }
}


impl Archive for ElasticArchive {

    fn archive(&self, slurm_job_entry: &SlurmJobEntry) -> Result<(), Error> {

        info!("Yup, got some {:?}", slurm_job_entry.jobid);

        let _res = self.client.document::<JobInfo>()
            .put_mapping()
            .send().unwrap();

        let doc = JobInfo {
            id: slurm_job_entry.jobid.to_owned(),
            script: String::from("test"),
            environment: HashMap::new(),
        };
        let _res = self.client.document()
            .index(doc)
            .send().unwrap();

        Ok(())
    }

}


#[derive(Serialize, Deserialize, ElasticType)]
struct JobInfo {
    #[elastic(id)]
    pub id: String,
    pub script: String,
    pub environment: HashMap<String, String>,
}