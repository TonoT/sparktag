package com.epoint.ztb.bigdata.tagmg.factory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.spark_project.jetty.util.StringUtil;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.alibaba.fastjson.JSON;
import com.epoint.ztb.bigdata.tagmg.annotation.Formula;
import com.epoint.ztb.bigdata.tagmg.common.PkgScanner;
import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;
import com.epoint.ztb.bigdata.tagmg.constants.SysConstant;
import com.epoint.ztb.bigdata.tagmg.iface.PreparedFormula;

public class FormulaFactory
{
    private static Map<String, PreparedFormula> formulas = new HashMap<String, PreparedFormula>();

    private static Logger logger = Logger.getLogger(FormulaFactory.class);

    public static void init() {
        if (formulas.isEmpty()) {
            PkgScanner scanner = new PkgScanner(SysConstant.PKG_NAME);
            List<String> classes = scanner.scan(Formula.class);
            for (String classname : classes) {
                try {
                    Class<?> clazz = Class.forName(classname);
                    if (PreparedFormula.class.isAssignableFrom(clazz)) {
                        String value = clazz.getAnnotation(Formula.class).value();
                        formulas.put(StringUtils.isNotBlank(value) ? value : classname,
                                (PreparedFormula) clazz.newInstance());
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static PreparedFormula getPreparedFormula(String name) {
        init();
        return formulas.get(name);
    }

    public static Object runFormula(String formula, String returntype, Map<String, Object> params) {
        // 如果返回值是浮点型 需要将所有的整型改为浮点型
        Object obj = null;

        if (ColumnType.浮点型.getValue().equals(returntype)) {
            for (Entry<String, Object> entry : params.entrySet()) {
                if (entry.getValue() instanceof Long) {
                    params.put(entry.getKey(), (long) entry.getValue() * 1.0);
                }
                else if (entry.getValue() instanceof Integer) {
                    params.put(entry.getKey(), (int) entry.getValue() * 1.0);
                }
            }
        }

        PreparedFormula pf = FormulaFactory.getPreparedFormula(formula);

        try {
            if (pf != null) {
                obj = pf.run(params, ColumnType.getEnumByValue(returntype));
            }
            else {
                ExpressionParser parser = new SpelExpressionParser();
                Expression exp = parser.parseExpression(formula);

                StandardEvaluationContext ctx = new StandardEvaluationContext();
                ctx.setVariables(params);

                // 除数为0时，结果是NaN
                obj = exp.getValue(ctx);
            }
        }
        catch (Exception e) {
            logger.error("公式计算发生异常！公式：" + formula + "，参数：" + JSON.toJSONString(params) + "，返回值类型：" + returntype, e);
            e.printStackTrace();
        }

        switch (ColumnType.getEnumByValue(returntype)) {
            case 整型:
            {
                String s = "";
                for (char c : obj.toString().toCharArray()) {
                    if (c >= 48 && c <= 57) {
                        s += c;
                    }
                    else {
                        break;
                    }
                }
                if (StringUtil.isNotBlank(s)) {
                    obj = Long.parseLong(s);
                }
                else {
                    obj = 0L;
                }
                break;
            }
            case 浮点型:
            {
                String s = "";
                boolean flag = true;
                for (char c : obj.toString().toCharArray()) {
                    if (c >= 48 && c <= 57) {
                        s += c;
                    }
                    else if (c == 46 && flag) {
                        s += c;
                        flag = false;
                    }
                    else {
                        break;
                    }
                }
                if (StringUtil.isNotBlank(s) && !".".equals(s)) {
                    obj = Double.parseDouble(s);
                }
                else {
                    obj = 0.0;
                }
                break;
            }
            case 时间型:
            case 字节型:
                break;
            default:
                obj = obj == null ? "" : obj.toString();
                break;
        }
        logger.debug("公式：" + formula + "，参数：" + JSON.toJSONString(params) + "，返回值类型：" + returntype + "，结果：" + obj);
        return obj;
    }

    public static Map<String, PreparedFormula> getFormulas() {
        return formulas;
    }
}
