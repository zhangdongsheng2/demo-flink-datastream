package cn.com.lrd.utils;

import com.commerce.commons.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 *
 */
public class ParameterToolUtil {
    private static ParameterTool PARAMETER_TOOL;

    public static ParameterTool getParameterTool() {
        if (PARAMETER_TOOL == null) {
            PARAMETER_TOOL = createParameterTool();
        }
        return PARAMETER_TOOL;
    }

    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return ParameterTool
                .fromPropertiesFile(ParameterToolUtil.class.getResourceAsStream("/" + parameterTool.get(PropertiesConstants.PROFILES_ACTIVE, PropertiesConstants.PROPERTIES_FILE_NAME)))
                .mergeWith(parameterTool)
                .mergeWith(ParameterTool.fromSystemProperties());
    }

    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(ParameterToolUtil.class.getResourceAsStream("/" + PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }


}