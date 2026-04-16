import pytest
from src.utils.ops import OwlWriter


def test_write_prefixes():
    writer = OwlWriter(base_uri="http://example.org/")
    writer.write_prefixes()

    # Should contain all prefixes
    output = writer.generate()
    assert "@prefix rdf:" in output
    assert "@prefix rdfs:" in output
    assert "@prefix owl:" in output
    assert "@prefix xsd:" in output
    assert "@prefix ex: <http://example.org/> ." in output

    # Ends with a blank line
    assert output.splitlines()[-1] == ""


def test_add_class():
    writer = OwlWriter()
    writer.add_class("Person")
    assert writer.generate().strip() == "ex:Person a owl:Class ."


def test_add_datatype_property():
    writer = OwlWriter()
    writer.add_datatype_property("Person", "age", "int")

    expected = (
        "ex:age a owl:DatatypeProperty ;\n"
        "    rdfs:domain ex:Person ;\n"
        "    rdfs:range xsd:integer ."
    )
    assert writer.generate().strip() == expected


def test_add_object_property():
    writer = OwlWriter()
    writer.add_object_property("knows", "Person", "Person")

    expected = (
        "ex:knows a owl:ObjectProperty ;\n"
        "    rdfs:domain ex:Person ;\n"
        "    rdfs:range ex:Person ."
    )
    assert writer.generate().strip() == expected


@pytest.mark.parametrize(
    "input_type, expected",
    [
        ("string", "string"),
        ("float", "float"),
        ("int", "integer"),
        ("date", "dateTime"),
        ("unknown", "string"),  # default fallback
    ]
)
def test_map_type(input_type, expected):
    writer = OwlWriter()
    assert writer.map_type(input_type) == expected


def test_generate_multiple_lines():
    writer = OwlWriter()
    writer.write_prefixes()
    writer.add_class("Person")
    writer.add_datatype_property("Person", "name", "string")

    output = writer.generate()
    lines = output.splitlines()

    # Should contain prefixes, class, and property
    assert any("ex:Person a owl:Class ." in line for line in lines)
    assert any("ex:name a owl:DatatypeProperty" in line for line in lines)

test_write_prefixes()
