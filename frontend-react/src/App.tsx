import { useEffect, useState } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import DashboardPage from "./pages/DashboardPage";
import RegisterPage from "./pages/RegisterPage";
import { getCurrentUser, subscribeAuthChanges } from "./services/authService";

function App() {
  const [currentUser, setCurrentUser] = useState(() => getCurrentUser());

  useEffect(() => {
    setCurrentUser(getCurrentUser());
    return subscribeAuthChanges(() => {
      setCurrentUser(getCurrentUser());
    });
  }, []);

  return (
    <Routes>
      <Route
        path="/register"
        element={currentUser ? <Navigate to="/dashboard" replace /> : <RegisterPage />}
      />
      <Route
        path="/dashboard"
        element={
          currentUser ? (
            <DashboardPage userEmail={currentUser.email} />
          ) : (
            <Navigate to="/register" replace />
          )
        }
      />
      <Route
        path="*"
        element={<Navigate to={currentUser ? "/dashboard" : "/register"} replace />}
      />
    </Routes>
  );
}

export default App;
