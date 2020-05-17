package com.commerce.commons.utils;


import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class DateUtil {

    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter YYYYMMddHHmmss = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");


    public static String formatTime(LocalDateTime dateTimeStr) {
        return dateTimeStr.format(YYYYMMddHHmmss);
    }

    public static String formatLocalDateTime(LocalDateTime dateTimeStr) {
        return dateTimeStr.format(DATETIME_FORMATTER);
    }

    /**
     * 返回当前的日期
     *
     * @return
     */
    public static LocalDate getCurrentLocalDate() {
        return LocalDate.now();
    }

    /**
     * 返回当前日期时间
     *
     * @return
     */
    public static LocalDateTime getCurrentLocalDateTime() {
        return LocalDateTime.now();
    }

    /**
     * 返回当前日期字符串 yyyy-MM-dd
     *
     * @return
     */
    public static String getCurrentDateStr() {
        return LocalDate.now().format(DATE_FORMATTER);
    }

    /**
     * 返回当前日期时间字符串 yyyy-MM-dd HH:mm:ss
     *
     * @return
     */
    public static String getCurrentDateTimeStr() {
        return LocalDateTime.now().format(DATETIME_FORMATTER);
    }

    //日期字符串格式化为 LocalDate
    public static LocalDate parseLocalDate(String dateStr, String pattern) {
        return LocalDate.parse(dateStr, DateTimeFormatter.ofPattern(pattern));
    }

    //日期字符串解析为 LocalDateTime
    public static LocalDateTime parseLocalDateTime(String dateTimeStr, String pattern) {
        return LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern(pattern));
    }

    public static LocalDateTime parseLocalDateTime(String dateTimeStr) {
        return LocalDateTime.parse(dateTimeStr, DATETIME_FORMATTER);
    }

    //yyyy-MM-dd HH:mm:ss 格式日期转换成毫秒
    public static long parseStrDateTime(String time) {
        return Timestamp.valueOf(LocalDateTime.parse(time, DATETIME_FORMATTER)).getTime();
    }

    //yyyy-MM-dd HH:mm:ss 格式日期转换成秒
    public static long parseStrDateTimeSec(String time) {
        return Timestamp.valueOf(LocalDateTime.parse(time, DATETIME_FORMATTER)).getTime() / 1000;
    }

    /**
     * 日期相隔天数
     *
     * @param startDateInclusive
     * @param endDateExclusive
     * @return
     */
    public static int periodDays(LocalDate startDateInclusive, LocalDate endDateExclusive) {
        return Period.between(startDateInclusive, endDateExclusive).getDays();
    }

    /**
     * 日期相隔小时
     *
     * @param startInclusive
     * @param endExclusive
     * @return
     */
    public static long durationHours(Temporal startInclusive, Temporal endExclusive) {
        return Duration.between(startInclusive, endExclusive).toHours();
    }

    /**
     * 日期相隔分钟
     *
     * @param startInclusive
     * @param endExclusive
     * @return
     */
    public static long durationMinutes(Temporal startInclusive, Temporal endExclusive) {
        return Duration.between(startInclusive, endExclusive).toMinutes();
    }

    /**
     * 日期相隔毫秒数
     *
     * @param startInclusive
     * @param endExclusive
     * @return
     */
    public static long durationMillis(Temporal startInclusive, Temporal endExclusive) {
        return Duration.between(startInclusive, endExclusive).toMillis();
    }

    /**
     * 是否当天
     *
     * @param date
     * @return
     */
    public static boolean isToday(LocalDate date) {
        return getCurrentLocalDate().equals(date);
    }

    /**
     * 获取本月的第一天
     *
     * @return
     */
    public static String getFirstDayOfThisMonth() {
        return getCurrentLocalDate().with(TemporalAdjusters.firstDayOfMonth()).format(DATE_FORMATTER);
    }

    /**
     * 获取本月的最后一天
     *
     * @return
     */
    public static String getLastDayOfThisMonth() {
        return getCurrentLocalDate().with(TemporalAdjusters.lastDayOfMonth()).format(DATE_FORMATTER);
    }

    /**
     * 获取2017-01的第一个周一
     *
     * @return
     */
    public static String getFirstMonday() {
        return LocalDate.parse("2017-01-01").with(TemporalAdjusters.firstInMonth(DayOfWeek.MONDAY))
                .format(DATE_FORMATTER);
    }

    /**
     * 获取当前日期的后两周
     *
     * @return
     */
    public static String getCurDateAfterTwoWeek() {
        return getCurrentLocalDate().plus(2, ChronoUnit.WEEKS).format(DATE_FORMATTER);
    }

    /**
     * 获取当前日期的6个月后的日期
     *
     * @return
     */
    public static String getCurDateAfterSixMonth() {
        return getCurrentLocalDate().plus(6, ChronoUnit.MONTHS).format(DATE_FORMATTER);
    }

    /**
     * 获取当前日期的5年后的日期
     *
     * @return
     */
    public static String getCurDateAfterFiveYear() {
        return getCurrentLocalDate().plus(5, ChronoUnit.YEARS).format(DATE_FORMATTER);
    }

    /**
     * 获取当前日期的20年后的日期
     *
     * @return
     */
    public static String getCurDateAfterTwentyYear() {
        return getCurrentLocalDate().plus(2, ChronoUnit.DECADES).format(DATE_FORMATTER);
    }

    /**
     * 时间戳转换成日期格式字符串
     *
     * @param seconds 精确到秒的字符串
     * @param format
     * @return
     */
    public static String timeStamp2Date(String seconds, String format) {
        if (StringUtils.isEmpty(seconds)) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.parseLong(seconds)));
    }


    //根据指定时间获取 时间所在的半小时的开始时间和结束时间
    public static List<LocalDateTime> getHalfStartTimeAndEdnTime(LocalDateTime localDateTime) {
        LocalDateTime startTime;
        LocalDateTime endTime;
        int minute = localDateTime.getMinute();
        if (minute == 10 || minute == 40) {
            startTime = localDateTime.plusMinutes(-10);
            endTime = localDateTime.plusMinutes(20);
        } else if (minute == 20 || minute == 50) {
            startTime = localDateTime.plusMinutes(-20);
            endTime = localDateTime.plusMinutes(10);
        } else {
            startTime = localDateTime.plusMinutes(-30);
            endTime = localDateTime;
        }
        List<LocalDateTime> localDateTimes = new ArrayList<>();
        localDateTimes.add(startTime);
        localDateTimes.add(endTime);
        return localDateTimes;
    }

    //获取指定时间的秒
    public static long toEpochSecond(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.of("+8"));
    }

    //获取指定时间的毫秒
    public static long toEpochMilli(LocalDateTime localDateTime) {
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
