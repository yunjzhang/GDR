package org.apache.gdr.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

@UDFType(deterministic = true)
@Description(name = "current_user1", value = "_FUNC_() - Returns current user name", extended = "SessionState UserFromAuthenticator")
public class CurrentUser extends GenericUDF {
    protected Text currentUser;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) {
        try {
            if (currentUser == null) {
                String user = System.getenv("SPARK_USER");
                user = StringUtils.isBlank(user) ? UserGroupInformation.getCurrentUser().getShortUserName()
                        : user;
                currentUser = new Text(user);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getLocalizedMessage());
        }

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return currentUser;
    }

    public Text getCurrentUser() {
        return currentUser;
    }

    public void setCurrentUser(Text currentUser) {
        this.currentUser = currentUser;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "CURRENT_USER()";
    }

    @Override
    public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
        super.copyToNewInstance(newInstance);
        // Need to preserve currentUser
        CurrentUser other = (CurrentUser) newInstance;
        if (this.currentUser != null) {
            other.currentUser = new Text(this.currentUser);
        }
    }
}
