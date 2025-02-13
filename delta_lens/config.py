from dataclasses import dataclass
from typing import List, Optional
import json
import jsonschema
import os

@dataclass
class Transform:
    query: str
    cached: bool = False

@dataclass
class Side:
    title: str
    inputFile: str
    transform: Optional[Transform] = None

@dataclass
class Entity:
    entityName: str
    leftSide: Side
    rightSide: Side
    primaryKeys: List[str]
    excludeColumns: Optional[List[str]] = None

@dataclass
class Defaults:
    leftSideTitle: str
    rightSideTitle: str
    filePattenGlobTemplate: str

@dataclass
class Config:
    defaults: Defaults
    entities: List[Entity]

def load_schema():
    schema_path = os.path.join(os.path.dirname(__file__), "config_schema.json")
    with open(schema_path, 'r') as f:
        return json.load(f)

def parse_config(config_path: str) -> Config:
    # Load and validate against schema
    schema = load_schema()
    
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    
    jsonschema.validate(instance=config_data, schema=schema)
    
    # Parse into objects
    defaults = Defaults(**config_data['defaults'])
    
    entities = []
    for entity_data in config_data['entities']:
        # Parse nested objects
        left_transform = Transform(**entity_data['leftSide'].get('transform', {})) if 'transform' in entity_data['leftSide'] else None
        right_transform = Transform(**entity_data['rightSide'].get('transform', {})) if 'transform' in entity_data['rightSide'] else None
        
        left_side = Side(**{**entity_data['leftSide'], 'transform': left_transform})
        right_side = Side(**{**entity_data['rightSide'], 'transform': right_transform})
        
        entity = Entity(
            entityName=entity_data['entityName'],
            leftSide=left_side,
            rightSide=right_side,
            primaryKeys=entity_data['primaryKeys'],
            excludeColumns=entity_data.get('excludeColumns')
        )
        entities.append(entity)
    
    return Config(defaults=defaults, entities=entities)