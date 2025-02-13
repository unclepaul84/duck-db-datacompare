from dataclasses import dataclass

from typing import List, Optional, Dict
import json
import re
import jsonschema
import os

@dataclass
class Transform:
    query: str
    cached: bool = False

@dataclass
class Side:
    title: str = None
    inputFile: str = None
    transform: Optional[Transform] = None

@dataclass
class Entity:
    entityName: str
    leftSide: Side
    rightSide: Side
    primaryKeys: List[str]
    dependencies: Optional[List[str]] = None
    excludeColumns: Optional[List[str]] = None

@dataclass
class Defaults:
    leftSideTitle: str = None
    rightSideTitle: str = None
    filePattenGlobTemplate: str = None

@dataclass
class ReferenceDataset:
    datasetName: str
    inputFile: str

@dataclass
class Config:
    entities: List[Entity]
    defaults:  Optional[Defaults]=None
    reference_datasets: Optional[List[ReferenceDataset]] = None
    @staticmethod
    def Validate(config):
        if config.defaults and config.defaults.leftSideTitle and config.defaults.rightSideTitle:
            Config._validate_title_characters(config.defaults.leftSideTitle, "leftSideTitle")
            Config._validate_title_characters(config.defaults.rightSideTitle, "rightSideTitle")    

        for entity in config.entities:
            if entity.leftSide.title and entity.rightSide.title:
                Config._validate_title_characters(entity.leftSide.title, "leftSide title")
                Config._validate_title_characters(entity.rightSide.title, "rightSide title")
        
        # Validate unique entity names
        Config._validate_unique_entity_names(config.entities)

        # Validate unique reference dataset names
        if config.reference_datasets:
            dataset_names = [ds.datasetName for ds in config.reference_datasets]
            duplicates = {name for name in dataset_names if dataset_names.count(name) > 1}
            if duplicates:
                raise ValueError(f"Duplicate reference dataset names found: {duplicates}")

        for entity in config.entities:
            Config._validate_entity_name_characters(entity.entityName)
   
    
    @staticmethod
    def _validate_unique_entity_names( entities) -> None:
        entity_names = [e.entityName for e in entities]
        duplicates = {name for name in entity_names if entity_names.count(name) > 1}
        if duplicates:
            raise ValueError(f"Duplicate entity names found: {duplicates}")
    @staticmethod
    def _validate_entity_name_characters( entity_name: str) -> None:
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', entity_name):
            raise ValueError(f"Invalid entity name '{entity_name}'. Entity names must start with a letter or underscore and can only contain letters, numbers, and underscores.")
    @staticmethod
    def _validate_title_characters( title: str, context: str) -> None:
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', title):
            raise ValueError(f"Invalid {context} '{title}'. Titles must start with a letter or underscore and can only contain letters, numbers, and underscores.")

class ConfigLoader:
    def __init__(self, schema_path: str):
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)

    def _create_transform(self, data: Dict) -> Transform:
        if not data:
            return None
        return Transform(
            query=data['query'],
            cached=data.get('cached', False)
        )

    def _create_side(self, data: Dict) -> Side:
        return Side(
            title=data['title'],
            inputFile=data['inputFile'],
            transform=self._create_transform(data.get('transform'))
        )

    def _create_entity(self, data: Dict) -> Entity:
        return Entity(
            entityName=data['entityName'],
            leftSide=self._create_side(data['leftSide']),
            rightSide=self._create_side(data['rightSide']),
            primaryKeys=data['primaryKeys'],
            dependencies=data.get('dependencies'),
            excludeColumns=data.get('excludeColumns')
        )

   
    def load_config(self, config_path: str) -> Config:
        """
        Load and validate configuration from a JSON file.
        
        Args:
            config_path: Path to the JSON configuration file
            
        Returns:
            Config: Validated configuration object
            
        Raises:
            jsonschema.ValidationError: If the configuration is invalid
            FileNotFoundError: If the config file doesn't exist
            ValueError: If duplicate entity names are found or invalid characters are used
        """
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        
        # Validate against schema
        jsonschema.validate(instance=config_data, schema=self.schema)
            
        # Validate entity name characters
          
       

        # Create config object
        config = Config(
            defaults=Defaults(**config_data['defaults']),
            entities=[self._create_entity(e) for e in config_data['entities']],
            reference_datasets=[ReferenceDataset(**r) for r in config_data.get('reference_datasets', [])]
    
        )
        Config.Validate(config)
        return config
    



def load_config(config_path: str, schema_path: Optional[str] = None) -> Config:
    """Helper function to load config with default schema location"""
    if schema_path is None:
        schema_path = os.path.join(os.path.dirname(__file__), "config_schema.json")
    loader = ConfigLoader(schema_path)
    return loader.load_config(config_path)