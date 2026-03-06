import "../stylesheets/base.css";
import "../stylesheets/api.css";
import { useAuth } from "../context/AuthContext";

const PLANS = [
  {
    name: "Free",
    price: "Free",
    perMinute: "60 API calls / minute",
    perMonth: "2,500 API calls / month",
    benefits: ["All GET endpoints", "Base rate limit", "Great for testing and small projects"],
  },
  {
    name: "Starter",
    price: "29€",
    perMinute: "300 API calls / minute",
    perMonth: "100,000 API calls / month",
    benefits: ["Higher throughput", "Prioritized processing", "Good for small products"],
  },
  {
    name: "Pro",
    price: "99€",
    perMinute: "1,000 API calls / minute",
    perMonth: "500,000 API calls / month",
    benefits: ["Supports high load", "Production-ready stability", "Extended scaling"],
  },
  {
    name: "Business",
    price: "149€",
    perMinute: "3,000 API calls / minute",
    perMonth: "3,000,000 API calls / month",
    benefits: ["Built for teams and platforms", "Very high rate limits", "B2B-ready usage"],
  },
  {
    name: "Enterprise",
    price: "Negotiable",
    perMinute: "Custom",
    perMonth: "Custom",
    benefits: ["Custom limits", "Custom conditions", "Direct integration support"],
  },
];

function normalizePlan(value) {
  return String(value || "").trim().toLowerCase();
}

function isSubscribedPlan(planName, userSubscription) {
  const normalizedPlan = normalizePlan(planName);
  const normalizedUser = normalizePlan(userSubscription);
  return normalizedPlan === normalizedUser;
}

function getPlanButtonLabel(planName, subscribed) {
  if (subscribed) {
    return "Subscribed";
  }
  if (planName === "Free") {
    return "Get API key";
  }
  if (planName === "Enterprise") {
    return "Contact";
  }
  return "Subscribe";
}

function ResourceDocCard({
  title,
  listEndpoint,
  detailEndpoint,
  description,
  fields,
  listResponse,
  detailResponse,
}) {
  return (
    <article className="api-endpoint-card">
      <header className="api-endpoint-head">
        <span className="api-method api-method-get">GET</span>
        <strong>{title}</strong>
      </header>
      <p>
        <strong>List:</strong> <code>{listEndpoint}</code>
      </p>
      <p>
        <strong>Detail:</strong> <code>{detailEndpoint}</code>
      </p>
      <p>{description}</p>
      <p>
        <strong>Auth:</strong> API key as query parameter <code>?api_key=YOUR_KEY</code>
      </p>
      <p>
        <strong>Status codes:</strong> 200 OK, 401 Unauthorized, 404 Not Found, 429 Too Many
        Requests, 500 Internal Server Error
      </p>
      <p>
        <strong>Important fields:</strong>
      </p>
      <ul className="api-doc-fields">
        {fields.map((field) => (
          <li key={field}>{field}</li>
        ))}
      </ul>
      <pre>{listResponse}</pre>
      <pre>{detailResponse}</pre>
    </article>
  );
}

export default function Api() {
  const { user, loggedIn } = useAuth();
  const userSubscription = user?.subscription;

  return (
    <div className="page-container api-page">
      <section className="api-hero">
        <h1>Ski API</h1>
        <p>
          Access resorts, slopes, and lifts with API key authentication and clear
          rate limits per subscription tier.
        </p>
      </section>

      <section className="api-pricing-section">
        <h2>Pricing / Subscription Tiers</h2>
        <div className="api-pricing-grid">
          {PLANS.map((plan) => {
            const subscribed = isSubscribedPlan(plan.name, userSubscription);
            const label = getPlanButtonLabel(plan.name, subscribed);
            const disabled = subscribed || (!loggedIn && plan.name !== "Free");
            const planClass = `plan-${plan.name.toLowerCase()}`;

            return (
              <article
                key={plan.name}
                className={`api-pricing-card ${planClass} ${subscribed ? "is-subscribed" : ""}`}
              >
                <h3>{plan.name}</h3>
                <p className="api-plan-price">{plan.price}</p>
                <p>{plan.perMinute}</p>
                <p>{plan.perMonth}</p>
                <ul>
                  {plan.benefits.map((benefit) => (
                    <li key={benefit}>{benefit}</li>
                  ))}
                </ul>
                <button
                  type="button"
                  className={`api-plan-button ${subscribed ? "is-subscribed" : ""}`}
                  disabled={disabled}
                  aria-label={`${plan.name} plan action`}
                >
                  {label}
                </button>
              </article>
            );
          })}
        </div>
        {!loggedIn && (
          <p className="api-login-hint">
            Note: Please log in for paid plans. The Free plan can be used directly with an API key.
          </p>
        )}
      </section>

      <section className="api-docs-section">
        <h2>API Documentation (GET, current state)</h2>
        <p>
          Base URL: <code>http://localhost:8080</code>
        </p>
        <p>
          This documentation focuses on read access for resorts, slopes, and lifts.
          All endpoints are GET endpoints and require a valid API key in the URL.
        </p>
        <p>
          Rate limits are enforced server-side in real time (per minute and per month).
          If exceeded, the API responds with <code>429</code>.
        </p>

        <div className="api-docs-grid">
          <ResourceDocCard
            title="Resorts"
            listEndpoint="/resorts?api_key=YOUR_KEY"
            detailEndpoint="/resorts/{id}?api_key=YOUR_KEY"
            description="Returns resort master data including location and altitude details."
            fields={[
              "id: string (e.g. resort slug)",
              "name: string",
              "country: string",
              "region: string | null",
              "continent: string | null",
              "latitude, longitude: number | null",
              "village_altitude_m, min_altitude_m, max_altitude_m: number | null",
              "ski_area_name, ski_area_type: string | null",
            ]}
            listResponse={`Example list:\n[{"id":"zermatt","name":"Zermatt","country":"CH","region":"Valais","continent":"Europe","latitude":46.02,"longitude":7.75,"village_altitude_m":1610,"min_altitude_m":1562,"max_altitude_m":3883,"ski_area_name":"Zermatt-Matterhorn","ski_area_type":"Alpine"}]`}
            detailResponse={`Example detail:\n{"id":"zermatt","name":"Zermatt","country":"CH","region":"Valais","continent":"Europe","latitude":46.02,"longitude":7.75,"village_altitude_m":1610,"min_altitude_m":1562,"max_altitude_m":3883,"ski_area_name":"Zermatt-Matterhorn","ski_area_type":"Alpine"}`}
          />

          <ResourceDocCard
            title="Slopes"
            listEndpoint="/slopes?api_key=YOUR_KEY"
            detailEndpoint="/slopes/{id}?api_key=YOUR_KEY"
            description="Returns resort slopes with difficulty level for routing and UI classification."
            fields={[
              "id: number",
              "resort_id: string (reference to Resort.id)",
              "name: string | null",
              "difficulty: string (e.g. blue, red, black)",
            ]}
            listResponse={`Example list:\n[{"id":12,"resort_id":"zermatt","name":"Sunnegga","difficulty":"blue"},{"id":13,"resort_id":"zermatt","name":"Furi Run","difficulty":"red"}]`}
            detailResponse={`Example detail:\n{"id":12,"resort_id":"zermatt","name":"Sunnegga","difficulty":"blue"}`}
          />

          <ResourceDocCard
            title="Lifts"
            listEndpoint="/lifts?api_key=YOUR_KEY"
            detailEndpoint="/lifts/{id}?api_key=YOUR_KEY"
            description="Returns lift infrastructure per resort including lift type for operations and map views."
            fields={[
              "id: number",
              "resort_id: string (reference to Resort.id)",
              "name: string | null",
              "lift_type: string (e.g. gondola, chairlift, draglift)",
            ]}
            listResponse={`Example list:\n[{"id":5,"resort_id":"zermatt","name":"Gondola A","lift_type":"gondola"},{"id":6,"resort_id":"zermatt","name":"Chairlift B","lift_type":"chairlift"}]`}
            detailResponse={`Example detail:\n{"id":5,"resort_id":"zermatt","name":"Gondola A","lift_type":"gondola"}`}
          />
        </div>
      </section>
    </div>
  );
}
