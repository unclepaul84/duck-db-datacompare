import pytest
from delta_lens.config import Config, Entity, Side, Transform, Defaults, ReferenceDataset, load_config
import json
import os
import tempfile

@pytest.fixture
def valid_transform():
    return Transform(query="SELECT * FROM trade", cached=True)

@pytest.fixture
def valid_side():
    return Side(
        title="system_1",
        inputFile="trades.csv",
        transform=Transform(query="SELECT * FROM trade", cached=True)
    )

@pytest.fixture
def valid_entity():
    return Entity(
        entityName="trade",
        leftSide=Side(title="system_1", inputFile="trades.csv"),
        rightSide=Side(title="system_2", inputFile="trades.csv"),
        primaryKeys=["trade_id"]
    )

@pytest.fixture
def valid_config_dict():
    return {
        "defaults": {
            "leftSideTitle": "system_1",
            "rightSideTitle": "system_2",
            "filePattenGlobTemplate": "{entityName}_{title}.csv"
        },
        "entities": [{
            "entityName": "trade",
            "leftSide": {
                "title": "system_1",
                "inputFile": "trades.csv",
                "transform": {
                    "query": "SELECT * FROM trade",
                    "cached": True
                }
            },
            "rightSide": {
                "title": "system_2",
                "inputFile": "trades.csv",
                "transform": {
                    "query": "SELECT * FROM trade",
                    "cached": True
                }
            },
            "primaryKeys": ["trade_id"]
        }],
        "reference_datasets": [{
            "datasetName": "instruments",
            "inputFile": "instruments.csv"
        }]
    }

@pytest.fixture
def config_file(valid_config_dict):
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        json.dump(valid_config_dict, f)
        return f.name

def test_validate_entity_name_valid():
    Config._validate_entity_name_characters("valid_name_123")
    Config._validate_entity_name_characters("_valid_name")
    Config._validate_entity_name_characters("ValidName")

def test_validate_entity_name_invalid():
    invalid_names = ["1invalid", "invalid-name", "invalid name", "invalid@name"]
    for name in invalid_names:
        with pytest.raises(ValueError, match="Invalid entity name"):
            Config._validate_entity_name_characters(name)

def test_validate_title_characters_valid():
    Config._validate_title_characters("valid_title_123", "test")
    Config._validate_title_characters("_valid_title", "test")
    Config._validate_title_characters("ValidTitle", "test")

def test_validate_title_characters_invalid():
    invalid_titles = ["1invalid", "invalid-title", "invalid title", "invalid@title"]
    for title in invalid_titles:
        with pytest.raises(ValueError, match="Invalid test"):
            Config._validate_title_characters(title, "test")

def test_validate_unique_entity_names(valid_side):
    entities = [
        Entity(entityName="trade1", leftSide=valid_side, rightSide=valid_side, primaryKeys=["id"]),
        Entity(entityName="trade2", leftSide=valid_side, rightSide=valid_side, primaryKeys=["id"])
    ]
    Config._validate_unique_entity_names(entities)

def test_validate_duplicate_entity_names(valid_side):
    entities = [
        Entity(entityName="trade", leftSide=valid_side, rightSide=valid_side, primaryKeys=["id"]),
        Entity(entityName="trade", leftSide=valid_side, rightSide=valid_side, primaryKeys=["id"])
    ]
    with pytest.raises(ValueError, match="Duplicate entity names found"):
        Config._validate_unique_entity_names(entities)

def test_load_config_valid(config_file):
    config = load_config(config_file)
    assert isinstance(config, Config)
    assert len(config.entities) == 1
    assert config.entities[0].entityName == "trade"
    os.unlink(config_file)

def test_load_config_invalid_entity_name(valid_config_dict, config_file):
    valid_config_dict["entities"][0]["entityName"] = "invalid-name"
    with open(config_file, 'w') as f:
        json.dump(valid_config_dict, f)
    with pytest.raises(ValueError, match="Invalid entity name"):
        load_config(config_file)
    os.unlink(config_file)

def test_load_config_duplicate_entities(valid_config_dict, config_file):
    valid_config_dict["entities"].append(valid_config_dict["entities"][0])
    with open(config_file, 'w') as f:
        json.dump(valid_config_dict, f)
    with pytest.raises(ValueError, match="Duplicate entity names found"):
        load_config(config_file)
    os.unlink(config_file)

def test_load_config_duplicate_reference_datasets(valid_config_dict, config_file):
    valid_config_dict["reference_datasets"].append(valid_config_dict["reference_datasets"][0])
    with open(config_file, 'w') as f:
        json.dump(valid_config_dict, f)
    with pytest.raises(ValueError, match="Duplicate reference dataset names found"):
        load_config(config_file)
    os.unlink(config_file)