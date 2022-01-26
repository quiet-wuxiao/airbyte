///*
// * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
// */
//
//package io.airbyte.integrations.standardtest.destination;
//
//import java.sql.Timestamp;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.time.Instant;
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.time.LocalTime;
//import java.time.ZoneId;
//import java.time.ZoneOffset;
//import java.time.ZonedDateTime;
//import java.time.format.DateTimeFormatter;
//import java.time.format.DateTimeParseException;
//import java.time.format.FormatStyle;
//import java.time.temporal.ChronoField;
//import java.util.Date;
//import java.util.TimeZone;
//
//public class DateTimeUtils {
//
//  public static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";
//  public static final String DATE_FORMAT_PATTERN_2 = "yyyy-MM-dd";
//  public static final String DATE_FORMAT_PATTERN_3 = "yyyy-MM-dd HH:mm:ss+00";
//  public static final DateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_PATTERN);
//  public static final DateFormat DATE_FORMAT_2 = new SimpleDateFormat(DATE_FORMAT_PATTERN_2);
//  public static final DateFormat DATE_FORMAT_3 = new SimpleDateFormat(DATE_FORMAT_PATTERN_3);
//
//  private static final DateTimeFormatter formatter =
//      DateTimeFormatter.ofPattern("[yyyy][yy]['-']['/']['.'][' '][MMM][MM][M]['-']['/']['.'][' '][dd][d]" +
//          "[[' ']['T']HH:mm[':'ss[.][SSSSSS][SSSSS][SSSS][SSS][' '][z][zzz][Z][O][x][XXX][XX][X]]]");
//  private static final DateTimeFormatter formatter2 =
//      DateTimeFormatter.ofPattern("[yyyy][yy]['-']['/']['.'][' '][MMM][MM][M]['-']['/']['.'][' '][dd][d]" +
//          "[[' ']['T']HH:mm[':'ss[.][SSSSSS][SSSSS][SSSS][SSS][' ']['+00']]]");
//  private static final DateTimeFormatter formatterWithoutTZ =
//      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:[':'ss[.][SSSSSS]+00");
//  private static final DateTimeFormatter timeFormatter =
//      DateTimeFormatter.ofPattern("HH:mm[':'ss[.][SSSSSS][SSSSS][SSSS][SSS]]");
//
//  /**
//   * Parse the Json date-time logical type to an Avro long value.
//   *
//   * @return the number of microseconds from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
//   */
//  public static Long getEpochMicros(String jsonDateTime) {
//    Instant instant = null;
//    if (jsonDateTime.matches("-?\\d+")) {
//      return Long.valueOf(jsonDateTime);
//    }
//    try {
//      ZonedDateTime zdt = ZonedDateTime.parse(jsonDateTime, formatter);
//      instant = zdt.toLocalDateTime().toInstant(ZoneOffset.of(zdt.getOffset().toString()));
//    } catch (DateTimeParseException e) {
//      try {
//        LocalDateTime dt = LocalDateTime.parse(jsonDateTime, formatter);
//        instant = dt.toInstant(ZoneOffset.UTC);
//      } catch (DateTimeParseException ex) {
//        // no logging since it may generate too much noise
//      }
//    }
//    return instant == null ? null : instant.toEpochMilli() * 1000;
//  }
//
//  public static String getParsed(String jsonDateTime) {
////    DateTimeFormatter.ISO_LOCAL_DATE_TIME
//    Instant instant = null;
//    try {
//      ZonedDateTime zdt = ZonedDateTime.parse(jsonDateTime, formatter);
//      instant = zdt.toLocalDateTime().atZone(ZoneId.systemDefault()).toLocalDateTime().toInstant(ZoneOffset.of(zdt.getOffset().toString()));
//      return DATE_FORMAT.format(new Date(instant.toEpochMilli() + (instant.getNano() / 1000000)));
//    } catch (DateTimeParseException e) {
//      try {
//        LocalDateTime dt = LocalDateTime.parse(jsonDateTime, formatter);
//        instant = dt.minusNanos(dt.getNano()).toInstant(ZoneOffset.ofHours(-10));
//      } catch (DateTimeParseException ex) {
//        // no logging since it may generate too much noise
//      }
//    }
//    return instant == null ? null : instant.toString();
//  }
//
//  public static String getParsed2(String jsonDateTime) {
////    DateTimeFormatter.ISO_LOCAL_DATE_TIME
//    Instant instant = null;
//    try {
//
//      ZonedDateTime zdt = ZonedDateTime.parse(jsonDateTime, formatter);
//      instant = zdt.toLocalDateTime().toInstant(ZoneOffset.ofHours((zdt.getOffset().get(ChronoField.OFFSET_SECONDS) /3600) + 2));
//      return DATE_FORMAT_3.format(new Date(instant.toEpochMilli() + (instant.getNano() / 1000000)));
//    } catch (DateTimeParseException e) {
//      try {
//        LocalDateTime dt = LocalDateTime.parse(jsonDateTime, formatter);
//        instant = dt.toInstant(ZoneOffset.ofHours(2));
//        return DATE_FORMAT_3.format(new Date(instant.toEpochMilli() + (instant.getNano() / 1000000)));
//      } catch (DateTimeParseException ex) {
//        // no logging since it may generate too much noise
//      }
//    }
//    return instant == null ? null : instant.toString();
//  }
//
//  /**
//   * Parse the Json date logical type to an Avro int.
//   *
//   * @return the number of days from the unix epoch, 1 January 1970 (ISO calendar).
//   */
//  public static Integer getEpochDay(String jsonDate) {
//    Integer epochDay = null;
//    try {
//      LocalDate date = LocalDate.parse(jsonDate, formatter);
//      epochDay = (int) date.toEpochDay();
//    } catch (DateTimeParseException e) {
//      // no logging since it may generate too much noise
//    }
//    return epochDay;
//  }
//
//  public static String convertToGeneralDateFormat2(String jsonDate) {
//    String epochDay = null;
//    try {
//      LocalDate date = LocalDate.parse(jsonDate, formatter);
//      epochDay = DATE_FORMAT.format(Date.from(date.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()));
//    } catch (DateTimeParseException e) {
//      // no logging since it may generate too much noise
//    }
//    return epochDay;
//  }
//
//  public static String convertToGeneralDateFormat3(String jsonDate) {
//    String epochDay = null;
//    try {
//      LocalDate date = LocalDate.parse(jsonDate, formatter);
//      epochDay = DATE_FORMAT_2.format(Date.from(date.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()));
//    } catch (DateTimeParseException e) {
//      // no logging since it may generate too much noise
//    }
//    return epochDay;
//  }
//
//  public static String convertToGeneralDateFormat(String jsonDate) {
//    String epochDay = null;
//    try {
//      LocalDate date = LocalDate.parse(jsonDate, formatter);
//      epochDay = date.toString();
//    } catch (DateTimeParseException e) {
//      // no logging since it may generate too much noise
//    }
//    return epochDay;
//  }
//
//  /**
//   * Parse the Json time logical type to an Avro long.
//   *
//   * @return the number of microseconds after midnight, 00:00:00.000000.
//   */
//  public static Long getMicroSeconds(String jsonTime) {
//    Long nanoOfDay = null;
//    if (jsonTime.matches("-?\\d+")) {
//      return Long.valueOf(jsonTime);
//    }
//    try {
//      LocalTime time = LocalTime.parse(jsonTime, timeFormatter);
//      nanoOfDay = time.toNanoOfDay();
//    } catch (DateTimeParseException e) {
//      try {
//        LocalTime time = LocalTime.parse(jsonTime, formatter);
//        nanoOfDay = time.toNanoOfDay();
//      } catch (DateTimeParseException ex) {
//        // no logging since it may generate too much noise
//      }
//    }
//    return nanoOfDay == null ? null : nanoOfDay / 1000;
//  }
//
//}
