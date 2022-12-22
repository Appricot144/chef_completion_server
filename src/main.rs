use mongodb::{
	error::Result,
	Client,
	options::{ClientOptions, FindOptions},
	bson::{doc, Document}
};
use serde::{Deserialize, Serialize};
use futures::stream::TryStreamExt;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Block {
	Statement(StatementBlock),
	Resource(ResourceBlock),
	Property(Property),
	Case(CaseBlock),
	If(IfBlock),
	When(Vec<String>),	 
	Elsif(Vec<String>),	
	Unknown(UnknownBlock),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnknownBlock {
	token1: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StatementBlock {
	statement_type: String,
	statement_name: String,
	contents: Vec<Block>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResourceBlock {
	resource_type: String,
	resource_name: String,
	contents: Vec<Block>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IfBlock {
	blocks: Vec<Block>,
	status: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CaseBlock {
	blocks: Vec<Block>,
	status: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Property {
	property: String,
	value: String,
}


#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Clone)]
struct PropertyEntity {
	property: String,
	count: i32,
}

enum Blk {
	Statement,
	Resource,
	Property,
	If,
	Case,
	Unknown,
}

struct CodeStructure {
	structure: Vec<Blk>
}

impl CodeStructure {
	fn new(code: &str) -> Self {
		let mut strct = Vec::new();

		for c in code.chars() {
			if c == 'S' { strct.push(Blk::Statement); }
			if c == 'R' { strct.push(Blk::Resource); }
			if c == 'P' { strct.push(Blk::Property); }
			if c == 'I' { strct.push(Blk::If); }
			if c == 'C' { strct.push(Blk::Case); }
			if c == 'U' { strct.push(Blk::Unknown); }
		}

		Self {
			structure: strct
		}
	}

	// ex. 	"Statement.contents.Resource"
	// 		"If.blocks.Resource"
	fn get_match_string(&self) -> String {
		let mut match_string = String::from("");
		
		for c in self.structure.iter() {
			match c {
				Blk::Statement 	=> { match_string.push_str("Statement.contents."); },
				Blk::Resource 	=> { match_string.push_str("Resource.contents."); },
				Blk::Property 	=> { },
				Blk::If 		=> { match_string.push_str("If.blocks."); },
				Blk::Case 		=> { match_string.push_str("Case.blocks."); },
				Blk::Unknown	=> { },
			}
		}

		// structure "SSC" "SI" などのIF CASEが末尾にくる並びに対しては、
		// 未実装
		
		// 最後の.contentsを削除
		match_string.pop();  // 末尾の . を削除
		let dot_offset =  match_string.rfind('.').unwrap_or(match_string.len());
		match_string.replace_range(dot_offset.., "");

		println!("Structure: {}",match_string);

		match_string
	}
}

// resource の property をカウントする関数
#[tokio::main]
async fn main() -> Result<()> {
	const DB_NAME: &str = "test";
	const DOC_NAME: &str = "test_collection";
	
	// get database handle 
	let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
	client_options.app_name = Some("Test Mongo App : ".to_string());
	let client = Client::with_options(client_options)?;
	let db = client.database(DB_NAME);
	
	// set resource name and structure
	let resource = "template";	// TEMPORARY
	const STRCT: &str = "SR";	// TEMPORARY

	// set find() condition
	let strct = CodeStructure::new(STRCT);
	let match_string = strct.get_match_string();
	let match_property = match_string.clone() + ".contents.Property.property"; //TODO
	let match_resource = match_string.clone() + ".resource_type"; //TODO

	// set find filter and options
	let filter = doc!{match_resource: resource};
	//let find_options = FindOptions::builder()
	//	.projection( doc! {"_id": 0, match_string: 1} )
	//	.build();

	// search entry
	let blk_collection = db.collection::<Block>(DOC_NAME);
	let mut cursor = blk_collection.find(filter, None).await?;
	
	// aggression property
	let mut property_count = HashMap::new();
	while let Some(block) = cursor.try_next().await? {

		// deserialize "Property"
		// count properties 
		match block {
			Block::Resource(res) => { //TODO
				for blk in res.contents {
					if let Block::Property(pro) = blk {
						let count =  property_count.entry(pro.property).or_insert(0);
						*count += 1;
					}
				}
			},
			_ => {}
		};
	}

	let mut property_count: Vec<(&String, &i32)> = property_count.iter().collect();

	// sort aggression result
	property_count.sort_by(|a, b| b.1.cmp(&a.1));
	for e in property_count.to_vec() {
		println!(": {:?}", e);
	}
	
	// find match entry
	let filter = doc! {
		"$and": [
			{ &match_property: &property_count[0].0 },
			{ &match_property: &property_count[1].0 },
			{ &match_property: &property_count[2].0 },
			{ &match_property: &property_count[3].0 },
		]
	};
	
	let block_collection = db.collection::<Document>(DOC_NAME);
	let mut cursor = block_collection.find(filter, None).await?;	
	while let Some(result) = cursor.try_next().await? {
		println!("{}", result);
	}
	
	Ok(())
}
