/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airbyte.commons.json.JsonSchemas;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.MoreLists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Helper class for Catalog and Stream related operations. Generally only used in tests.
 */
public class CatalogHelpers {

  public static AirbyteCatalog createAirbyteCatalog(final String streamName, final Field... fields) {
    return new AirbyteCatalog().withStreams(Lists.newArrayList(createAirbyteStream(streamName, fields)));
  }

  public static AirbyteStream createAirbyteStream(final String streamName, final Field... fields) {
    // Namespace is null since not all sources set it.
    return createAirbyteStream(streamName, null, Arrays.asList(fields));
  }

  public static AirbyteStream createAirbyteStream(final String streamName, final String namespace, final Field... fields) {
    return createAirbyteStream(streamName, namespace, Arrays.asList(fields));
  }

  public static AirbyteStream createAirbyteStream(final String streamName, final String namespace, final List<Field> fields) {
    return new AirbyteStream().withName(streamName).withNamespace(namespace).withJsonSchema(fieldsToJsonSchema(fields));
  }

  public static ConfiguredAirbyteCatalog createConfiguredAirbyteCatalog(final String streamName, final String namespace, final Field... fields) {
    return new ConfiguredAirbyteCatalog().withStreams(Lists.newArrayList(createConfiguredAirbyteStream(streamName, namespace, fields)));
  }

  public static ConfiguredAirbyteCatalog createConfiguredAirbyteCatalog(final String streamName, final String namespace, final List<Field> fields) {
    return new ConfiguredAirbyteCatalog().withStreams(Lists.newArrayList(createConfiguredAirbyteStream(streamName, namespace, fields)));
  }

  public static ConfiguredAirbyteStream createConfiguredAirbyteStream(final String streamName, final String namespace, final Field... fields) {
    return createConfiguredAirbyteStream(streamName, namespace, Arrays.asList(fields));
  }

  public static ConfiguredAirbyteStream createConfiguredAirbyteStream(final String streamName, final String namespace, final List<Field> fields) {
    return new ConfiguredAirbyteStream()
        .withStream(new AirbyteStream().withName(streamName).withNamespace(namespace).withJsonSchema(fieldsToJsonSchema(fields)))
        .withSyncMode(SyncMode.FULL_REFRESH).withDestinationSyncMode(DestinationSyncMode.OVERWRITE);
  }

  /**
   * Convert a Catalog into a ConfiguredCatalog. This applies minimum default to the Catalog to make
   * it a valid ConfiguredCatalog.
   *
   * @param catalog - Catalog to be converted.
   * @return - ConfiguredCatalog based of off the input catalog.
   */
  public static ConfiguredAirbyteCatalog toDefaultConfiguredCatalog(final AirbyteCatalog catalog) {
    return new ConfiguredAirbyteCatalog()
        .withStreams(catalog.getStreams()
            .stream()
            .map(CatalogHelpers::toDefaultConfiguredStream)
            .collect(Collectors.toList()));
  }

  public static ConfiguredAirbyteStream toDefaultConfiguredStream(final AirbyteStream stream) {
    return new ConfiguredAirbyteStream()
        .withStream(stream)
        .withSyncMode(SyncMode.FULL_REFRESH)
        .withCursorField(new ArrayList<>())
        .withDestinationSyncMode(DestinationSyncMode.OVERWRITE)
        .withPrimaryKey(new ArrayList<>());
  }

  public static JsonNode fieldsToJsonSchema(final Field... fields) {
    return fieldsToJsonSchema(Arrays.asList(fields));
  }

  /**
   * Maps a list of fields into a JsonSchema object with names and types. This method will throw if it
   * receives multiple fields with the same name.
   *
   * @param fields fields to map to JsonSchema
   * @return JsonSchema representation of the fields.
   */
  public static JsonNode fieldsToJsonSchema(final List<Field> fields) {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("type", "object")
        .put("properties", fields
            .stream()
            .collect(Collectors.toMap(
                Field::getName,
                field -> {
                  if (isObjectWithSubFields(field)) {
                    return fieldsToJsonSchema(field.getSubFields());
                  } else {
                    return field.getType().getJsonSchemaTypeMap();
                  }
                })))
        .build());
  }

  /**
   * Gets the keys from the top-level properties object in the json schema.
   *
   * @param stream - airbyte stream
   * @return field names
   */
  @SuppressWarnings("unchecked")
  public static Set<String> getTopLevelFieldNames(final ConfiguredAirbyteStream stream) {
    // it is json, so the key has to be a string.
    final Map<String, Object> object = Jsons.object(stream.getStream().getJsonSchema().get("properties"), Map.class);
    return object.keySet();
  }

  /**
   * @param node any json node
   * @return a set of all keys for all objects within the node
   */
  @VisibleForTesting
  protected static Set<String> getAllFieldNames(final JsonNode node) {
    return getFullyQualifiedFieldNames(node)
        .stream()
        .map(MoreLists::last)
        .flatMap(Optional::stream)
        .collect(Collectors.toSet());
//    final Set<String> allFieldNames = new HashSet<>();
//
//    if (node.has("properties")) {
//      final JsonNode properties = node.get("properties");
//      final Iterator<String> fieldNames = properties.fieldNames();
//      while (fieldNames.hasNext()) {
//        final String fieldName = fieldNames.next();
//        allFieldNames.add(fieldName);
//        final JsonNode fieldValue = properties.get(fieldName);
//        if (fieldValue.isObject()) {
//          allFieldNames.addAll(getAllFieldNames(fieldValue));
//        }
//      }
//    }
//
//    return allFieldNames;
  }

  protected static Set<List<String>> getFullyQualifiedFieldNames(final JsonNode node) {
    return getFullyQualifiedFieldNames(node, new ArrayList<>());
  }

  /**
   * @param node any json node
   * @return a set of all keys for all objects within the node
   */
  @VisibleForTesting
  protected static Set<List<String>> getFullyQualifiedFieldNames(final JsonNode node, final List<String> pathToNode) {
    final Set<List<String>> allFieldNames = new HashSet<>();

    if (node.has("properties")) {
      final JsonNode properties = node.get("properties");
      final Iterator<String> fieldNames = properties.fieldNames();
      while (fieldNames.hasNext()) {
        final String fieldName = fieldNames.next();
        final ArrayList<String> pathWithThisField = new ArrayList<>(pathToNode);
        pathWithThisField.add(fieldName);
        allFieldNames.add(pathWithThisField);
        final JsonNode fieldValue = properties.get(fieldName);
        if (fieldValue.isObject()) {
          allFieldNames.addAll(getFullyQualifiedFieldNames(fieldValue, pathWithThisField));
        }
      }
    }

    return allFieldNames;
  }


  protected static Set<Pair<List<String>, JsonSchemaType>>  getFullyQualifiedFieldNamesWithTypes(final JsonNode node) {
    return getFullyQualifiedFieldNamesWithTypes(node, new ArrayList<>());
  }

  /**
   * @param node any json node
   * @return a set of all keys for all objects within the node
   */
  @VisibleForTesting
  protected static Set<Pair<List<String>, JsonSchemaType>> getFullyQualifiedFieldNamesWithTypes(final JsonNode node, final List<String> pathToNode) {
    final Set<Pair<List<String>, JsonSchemaType>> allFieldNames = new HashSet<>();

    if (node.has("properties")) {
      final JsonNode properties = node.get("properties");
      final Iterator<Map.Entry<String, JsonNode>> fieldNames = properties.fields();
      while (fieldNames.hasNext()) {
        final Map.Entry<String, JsonNode> entry = fieldNames.next();
        final String fieldName = entry.getKey();
        // todo hack, hack hack.
        // figure out how to build jsonschema type properly
        // including how to parse jsonschema primitive properly
        // figure out how to handle array schema types (or should JsonSchemaType => List<JsonSchemaType>)?
        final JsonSchemaType fieldType = JsonSchemaType.builder(JsonSchemaPrimitive.valueOf(JsonSchemas.getType(entry.getValue()).get(0))).build();
        final ArrayList<String> pathWithThisField = new ArrayList<>(pathToNode);
        pathWithThisField.add(fieldName);
        allFieldNames.add(Pair.of(pathWithThisField, fieldType));
        final JsonNode fieldValue = properties.get(fieldName);
        if (fieldValue.isObject()) {
          allFieldNames.addAll(getFullyQualifiedFieldNamesWithTypes(fieldValue, pathWithThisField));
        }
      }
    }

    return allFieldNames;
  }

  private static boolean isObjectWithSubFields(final Field field) {
    return field.getType() == JsonSchemaType.OBJECT && field.getSubFields() != null && !field.getSubFields().isEmpty();
  }

  public static StreamDescriptor extractStreamDescriptor(final AirbyteStream airbyteStream) {
    return new StreamDescriptor().withName(airbyteStream.getName()).withNamespace(airbyteStream.getNamespace());
  }

  private static Map<StreamDescriptor, AirbyteStream> streamDescriptorToMap(final AirbyteCatalog catalog) {
    return catalog.getStreams()
        .stream()
        .collect(Collectors.toMap(CatalogHelpers::extractStreamDescriptor, s -> s));
  }

  /**
   * Returns information about was has changed.
   * @param oldCatalog
   * @param newCatalog
   * @return
   */
  private static CatalogDiff getDiff(final AirbyteCatalog oldCatalog, final AirbyteCatalog newCatalog) {
    final List<StreamTransform> streamTransforms = new ArrayList<>();

    final Map<StreamDescriptor, AirbyteStream> descriptorToStreamOld = streamDescriptorToMap(oldCatalog);
    final Map<StreamDescriptor, AirbyteStream> descriptorToStreamNew = streamDescriptorToMap(newCatalog);

    Sets.difference(descriptorToStreamOld.keySet(), descriptorToStreamNew.keySet())
        .forEach(descriptor -> streamTransforms.add(StreamTransform.createRemoveStreamTransform(new RemoveStreamTransform(descriptor))));
    Sets.difference(descriptorToStreamNew.keySet(), descriptorToStreamOld.keySet())
        .forEach(descriptor -> streamTransforms.add(StreamTransform.createAddStreamTransform(new AddStreamTransform(descriptor))));
    Sets.union(descriptorToStreamOld.keySet(), descriptorToStreamNew.keySet())
        .forEach(descriptor -> {
          final AirbyteStream streamOld = descriptorToStreamOld.get(descriptor);
          final AirbyteStream streamNew = descriptorToStreamNew.get(descriptor);
          if(!streamOld.equals(streamNew)) {
            final List<FieldTransform> fieldTransforms = new ArrayList<>();
            final Map<List<String>, JsonSchemaType> fieldNameToTypeOld = getFullyQualifiedFieldNamesWithTypes(streamOld.getJsonSchema())
                .stream()
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            final Map<List<String>, JsonSchemaType> fieldNameToTypeNew = getFullyQualifiedFieldNamesWithTypes(streamOld.getJsonSchema())
                .stream()
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

            Sets.difference(fieldNameToTypeOld.keySet(), fieldNameToTypeNew.keySet())
                .forEach(fieldName -> fieldTransforms.add(FieldTransform.createRemoveFieldTransform(new RemoveFieldTransform(fieldName))));
            Sets.difference(fieldNameToTypeNew.keySet(), fieldNameToTypeOld.keySet())
                .forEach(fieldName -> fieldTransforms.add(FieldTransform.createAddFieldTransform(new AddFieldTransform(fieldName))));
            Sets.union(fieldNameToTypeOld.keySet(), fieldNameToTypeNew.keySet()).forEach(fieldName -> {
              // get type of each, if not equal, diff them
              final JsonSchemaType oldType = fieldNameToTypeOld.get(fieldName);
              final JsonSchemaType newType = fieldNameToTypeNew.get(fieldName);

              if(!oldType.equals(newType)) {
                fieldTransforms.add(FieldTransform.createUpdateFieldTransform(new UpdateFieldTransform(fieldName, oldType, newType)));
              }
            });

            streamTransforms.add(StreamTransform.createUpdateStreamTransform(new UpdateStreamTransform(descriptor, fieldTransforms)));
          }
        });

    return new CatalogDiff(streamTransforms);
  }


  enum StreamTransformType {
    ADD_STREAM,
    REMOVE_STREAM,
    UPDATE_STREAM
  }

  private record CatalogDiff(List<StreamTransform> streamTransforms) {

    public List<StreamTransform> getStreamTransforms() {
      return new ArrayList<>(streamTransforms);
    }
  }

  private static class StreamTransform {
    private final StreamTransformType transformType;

    private final AddStreamTransform addStreamTransform;
    private final RemoveStreamTransform removeStreamTransform;
    private final UpdateStreamTransform updateStreamTransform;

    static StreamTransform createAddStreamTransform(final AddStreamTransform addStreamTransform) {
      return new StreamTransform(StreamTransformType.ADD_STREAM, addStreamTransform, null, null);
    }

    static StreamTransform createRemoveStreamTransform(final RemoveStreamTransform removeStreamTransform) {
      return new StreamTransform(StreamTransformType.REMOVE_STREAM, null, removeStreamTransform, null);
    }

    static StreamTransform createUpdateStreamTransform(final UpdateStreamTransform updateStreamTransform) {
      return new StreamTransform(StreamTransformType.UPDATE_STREAM, null, null, updateStreamTransform);
    }

    private StreamTransform(final StreamTransformType transformType, final AddStreamTransform addStreamTransform,
        final RemoveStreamTransform removeStreamTransform, final UpdateStreamTransform updateStreamTransform) {
      this.transformType = transformType;
      this.addStreamTransform = addStreamTransform;
      this.removeStreamTransform = removeStreamTransform;
      this.updateStreamTransform = updateStreamTransform;
    }

    public StreamTransformType getTransformType() {
      return transformType;
    }

    public AddStreamTransform getAddStreamTransform() {
      return addStreamTransform;
    }

    public RemoveStreamTransform getRemoveStreamTransform() {
      return removeStreamTransform;
    }

    public UpdateStreamTransform getUpdateStreamTransform() {
      return updateStreamTransform;
    }
  }

  // todo (cgardens) shouldn't extend this.
  private static class UpdateStreamTransform extends StreamNameTransform {
    private final List<FieldTransform> fieldTransforms;

    private UpdateStreamTransform(final StreamDescriptor streamDescriptor, final List<FieldTransform> fieldTransforms) {
      super(streamDescriptor);
      this.fieldTransforms = fieldTransforms;
    }

    public List<FieldTransform> getFieldTransforms() {
      return fieldTransforms;
    }
  }

  private static class AddStreamTransform extends StreamNameTransform{

    private AddStreamTransform(final StreamDescriptor streamDescriptor) {
      super(streamDescriptor);
    }
  }

  private static class RemoveStreamTransform extends StreamNameTransform{

    private RemoveStreamTransform(final StreamDescriptor streamDescriptor) {
      super(streamDescriptor);
    }
  }

  private static class StreamNameTransform {
    private final StreamDescriptor streamDescriptor;

    private StreamNameTransform(final StreamDescriptor streamDescriptor) {
      this.streamDescriptor = streamDescriptor;
    }

    public StreamDescriptor getStreamDescriptor() {
      return streamDescriptor;
    }
  }

  enum FieldTransformType {
    ADD_FIELD,
    REMOVE_FIELD,
    UPDATE_FIELD
  }

  private static class FieldTransform {
    private final FieldTransformType transformType;

    private final AddFieldTransform addFieldTransform;
    private final RemoveFieldTransform removeFieldTransform;
    private final UpdateFieldTransform updateFieldTransform;

    static FieldTransform createAddFieldTransform(final AddFieldTransform addFieldTransform) {
      return new FieldTransform(FieldTransformType.ADD_FIELD, addFieldTransform, null, null);
    }

    static FieldTransform createRemoveFieldTransform(final RemoveFieldTransform removeFieldTransform) {
      return new FieldTransform(FieldTransformType.REMOVE_FIELD, null, removeFieldTransform, null);
    }

    static FieldTransform createUpdateFieldTransform(final UpdateFieldTransform updateFieldTransform) {
      return new FieldTransform(FieldTransformType.UPDATE_FIELD, null, null, updateFieldTransform);
    }

    private FieldTransform(final FieldTransformType transformType, final AddFieldTransform addFieldTransform,
        final RemoveFieldTransform removeFieldTransform, final UpdateFieldTransform updateFieldTransform) {
      this.transformType = transformType;
      this.addFieldTransform = addFieldTransform;
      this.removeFieldTransform = removeFieldTransform;
      this.updateFieldTransform = updateFieldTransform;
    }

    public FieldTransformType getTransformType() {
      return transformType;
    }

    public AddFieldTransform getAddFieldTransform() {
      return addFieldTransform;
    }

    public RemoveFieldTransform getRemoveFieldTransform() {
      return removeFieldTransform;
    }

    public UpdateFieldTransform getUpdateFieldTransform() {
      return updateFieldTransform;
    }
  }

  // todo (cgardens) shouldn't extend this.
  private static class UpdateFieldTransform extends FieldNameTransform {
    private final JsonSchemaType oldType;
    private final JsonSchemaType newType;

    private UpdateFieldTransform(final List<String> fieldName, final JsonSchemaType oldType, final JsonSchemaType newType) {
      super(fieldName);
      this.oldType = oldType;
      this.newType = newType;
    }

    public JsonSchemaType getOldType() {
      return oldType;
    }

    public JsonSchemaType getNewType() {
      return newType;
    }
  }

  private static class AddFieldTransform extends FieldNameTransform{

    private AddFieldTransform(final List<String> fieldName) {
      super(fieldName);
    }
  }

  private static class RemoveFieldTransform extends FieldNameTransform{

    private RemoveFieldTransform(final List<String> fieldName) {
      super(fieldName);
    }
  }

  private static class FieldNameTransform {
    private final List<String> fieldName;

    private FieldNameTransform(final List<String> fieldName) {
      this.fieldName = fieldName;
    }

    public List<String> getFieldName() {
      return fieldName;
    }
  }
}
