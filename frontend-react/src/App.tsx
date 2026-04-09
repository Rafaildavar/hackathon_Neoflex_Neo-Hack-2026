import { Navigate, Route, Routes } from "react-router-dom";
import DashboardPage from "./pages/DashboardPage";
import RegisterPage from "./pages/RegisterPage";
import { getCurrentUser } from "./services/authService";

function App() {
  const currentUser = getCurrentUser();

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
