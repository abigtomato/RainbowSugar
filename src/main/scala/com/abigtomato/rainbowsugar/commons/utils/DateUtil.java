package com.abigtomato.rainbowsugar.commons.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Date;

public class DateUtil {

    /**
     * LocalDate转Date
     */
    public static Date asDate(LocalDate localDate) {
        return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * LocalDateTime转Date
     */
    public static Date asDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * Date转LocalDate
     */
    public static LocalDate asLocalDate(Date date) {
        return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
    }

    /**
     * Date转LocalDateTime
     */
    public static LocalDateTime asLocalDateTime(Date date) {
        return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * LocalDateTime转String
     */
    public static String toStr(LocalDateTime localDateTime) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(localDateTime);
    }

    /**
     * LocalDateTime转String 2
     */
    public static String toStr2(LocalDateTime localDateTime) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(localDateTime);
    }

    /**
     * Date转string
     */
    public static String toStr(Date date) {
        LocalDateTime localDateTime = asLocalDateTime(date);
        return toStr(localDateTime);
    }

    /**
     * LocalDate转String
     */
    public static String toDateStr(LocalDate localDate) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(localDate);
    }

    /**
     * String转LocalDateTime
     */
    public static LocalDateTime toLocalDateTime(String str) {
        return LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * 获取本月第一天日期 LocalDate
     */
    public static LocalDate toMonthFirstDate(LocalDate localDate) {
        return localDate.with(TemporalAdjusters.firstDayOfMonth());
    }

    /**
     * 获取本月最后一天日期 LocalDate
     */
    public static LocalDate toMonthLastDate(LocalDate localDate) {
        return localDate.with(TemporalAdjusters.lastDayOfMonth());
    }

    /**
     * 生成起始时间 LocalDateTime
     */
    public static LocalDateTime ofStart(LocalDate localDate) {
        return LocalDateTime.of(localDate, LocalTime.of(0, 0, 0));
    }

    /**
     * 生成结束时间 LocalDateTime
     */
    public static LocalDateTime ofEnd(LocalDate localDate) {
        return LocalDateTime.of(localDate, LocalTime.of(23, 59, 59));
    }

    /**
     * 计算间隔天数 LocalDate
     */
    public static long between(LocalDate before, LocalDate after) {
        return ChronoUnit.DAYS.between(before, after);
    }

    /**
     * 生成今日起始时间 String
     */
    public static String ofTodayStart() {
        return toStr(DateUtil.ofStart(LocalDate.now()));
    }

    /**
     * 生成今日结束时间 String
     */
    public static String ofTodayEnd() {
        return toStr(DateUtil.ofEnd(LocalDate.now()));
    }

    /**
     * 生成昨日起始时间 String
     */
    public static String ofYesterdayStart() {
        return toStr(DateUtil.ofStart(LocalDate.now().plusDays(-1)));
    }

    /**
     * 生成昨日结束时间 String
     */
    public static String ofYesterdayEnd() {
        return toStr(DateUtil.ofEnd(LocalDate.now().plusDays(-1)));
    }

    /**
     * 生成本月起始时间 String
     */
    public static String ofMonthStart() {
        LocalDate today = LocalDate.now();
        LocalDate monthFirstDate = toMonthFirstDate(today);
        LocalDateTime monthFirstDateTime = ofStart(monthFirstDate);
        return toStr(monthFirstDateTime);
    }

    /**
     * 生成本月结束时间 String
     */
    public static String ofMonthEnd() {
        LocalDate today = LocalDate.now();
        LocalDate monthLastDate = toMonthLastDate(today);
        LocalDateTime monthLastDateTime = ofEnd(monthLastDate);
        return toStr(monthLastDateTime);
    }

    /**
     * 生成前7天起始时间 String
     */
    public static String ofNearly7DaysStart() {
        return toStr(DateUtil.ofStart(LocalDate.now().plusDays(-7)));
    }

    /**
     * 生成前15天起始时间 String
     */
    public static String ofNearly15DaysStart() {
        return toStr(DateUtil.ofStart(LocalDate.now().plusDays(-15)));
    }

    /**
     * 生成前30天起始时间 String
     */
    public static String ofNearly30DaysStart() {
        return toStr(DateUtil.ofStart(LocalDate.now().plusDays(-30)));
    }
}
