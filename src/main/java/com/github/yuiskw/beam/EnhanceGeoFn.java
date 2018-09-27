/**
 * Copyright (c) 2017 Yu Ishikawa.
 */
package com.github.yuiskw.beam;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.*;

import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.*;
import com.google.protobuf.Timestamp;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

import com.google.maps.*;
import com.google.maps.model.*;


/**
 * This class is an Apache Beam function enhance an Entity with Latitude and Longitude information using the Google Maps API
 */
public class EnhanceGeoFn extends DoFn<Entity, Entity> {
  /** Google Cloud Platform project ID */
  private String projectId;
  /** Google Datastore name space */
  private String namespace;
  /** Google Datastore parent paths */
  private LinkedHashMap<String, String> parents;
  /** Google Datastore kind name */
  private String kind;
  /** BigQuery column for Google Datastore key */
  private String keyColumn;
  /** Indexed columns in Google Datastore */
  private List<String> indexedColumns;
  /** Maps API Key */
  private String mapsApiKey;

  public EnhanceGeoFn(
      String projectId,
      String namespace,
      LinkedHashMap<String, String> parents,
      String kind,
      String keyColumn,
      List<String> indexedColumns,
      String mapsApiKey) {
    this.projectId = projectId;
    this.namespace = namespace;
    this.parents = parents;
    this.kind = kind;
    this.keyColumn = keyColumn;
    this.indexedColumns = indexedColumns;
    this.mapsApiKey = mapsApiKey;
  }

    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        Entity entity = c.element();
        Entity.Builder builder = Entity.newBuilder(entity);
        Map<String, Value> fields = entity.getPropertiesMap();

        String addr1 = String.valueOf(fields.get("provider_first_line_business_practice_location_address"));
        String addrCity = String.valueOf(fields.get("provider_business_practice_location_address_city_name"));
        String addrState = String.valueOf(fields.get("provider_business_practice_location_address_state_name"));
        String addrZip = String.valueOf(fields.get("provider_business_practice_location_address_postal_code"));

        String address = addr1 + ", " + addrCity + ", " + addrState + " " + addrZip;
        /**  entity.getProperty("provider_first_line_business_practice_location_address") + "," +
          entity.getProperty("provider_business_practice_location_address_state_name") + " " +
          entity.getProperty("provider_business_practice_location_address_postal_code"); */
        GeoApiContext context = new GeoApiContext.Builder()
          .apiKey(this.mapsApiKey)
          .build();
        GeocodingResult[] results =  GeocodingApi.geocode(context,
          address).await();
        Double longitude = results[0].geometry.location.lng;
        Double latitude = results[0].geometry.location.lat;
        Value vlat = Value.newBuilder().setDoubleValue(latitude).setExcludeFromIndexes(true).build();
        Value vlon = Value.newBuilder().setDoubleValue(longitude).setExcludeFromIndexes(true).build();
        builder.putProperties("longitude",vlat);
        builder.putProperties("latitude",vlon);
        c.output(builder.build());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
}
